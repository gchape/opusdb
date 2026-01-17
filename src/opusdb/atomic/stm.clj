(ns opusdb.atomic.stm
  (:refer-clojure :exclude [ref deref ref-set alter dosync sync])
  (:require
   [opusdb.atomic.lifecycle.events :as events])
  (:import
   [java.util Collections HashMap]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicInteger AtomicLong]))

(def ^:private MAX_HISTORY 16)
(def ^:private COMMIT_LOCK (Object.))
(def ^:private ^AtomicLong WRITE_POINT (AtomicLong.))
(def ^:private ^AtomicLong TRANSACTION_ID (AtomicLong.))
(def ^:private ^ConcurrentHashMap ACTIVE_TRANSACTIONS (ConcurrentHashMap.))

(def ^{:private true :dynamic true} *current-transaction* nil)

(defn- make-transaction []
  (let [read-point (.get WRITE_POINT)
        tx-id (.incrementAndGet TRANSACTION_ID)
        tx {:id tx-id
            :read-set (HashMap.)
            :write-set (HashMap.)
            :read-point read-point
            :retry-count (AtomicInteger.)
            :status (volatile! ::ACTIVE)}]
    (.put ACTIVE_TRANSACTIONS tx-id tx)
    tx))

(defn- retry []
  (throw (ex-info "Transaction retry" {:type ::ABORTED})))

(defn- retry-if-aborted [tx]
  (when (= @(:status tx) ::ABORTED)
    (retry)))

(defn- ensure-read-consistency [read-set]
  (doseq [[ref entry] read-set]
    (when (> (:write-point @ref) (:write-point entry))
      (retry))))

(defn- apply-writes! [write-set]
  (let [write-point' (.incrementAndGet WRITE_POINT)]
    (doseq [[ref val] write-set]
      (swap! ref
             (fn [{:keys [history] :as state}]
               (let [history' (->> (conj history
                                         {:value val :write-point write-point'})
                                   (take-last MAX_HISTORY)
                                   vec)]
                 (merge state {:history history'
                               :write-point write-point'
                               :owner nil})))))))

(defn- find-version [read-point history]
  (let [index (Collections/binarySearch history
                                        {:write-point read-point}
                                        #(compare
                                          (:write-point %1)
                                          (:write-point %2)))]
    (if (>= index 0)
      (history index)
      (let [index' (- (inc index))]
        (when (> index' 0)
          (history (dec index')))))))

(defn- acquire-ownership [ref tx]
  (let [lock (:lock @ref)]
    (monitor-enter lock)
    (try
      (let [thief (:id tx)
            owner (:owner @ref)]
        (if (or (nil? owner) (>= thief owner))
          (do
            (when (and owner (> thief owner))
              (when-some [owner-tx (.get ACTIVE_TRANSACTIONS owner)]
                (when (= @(:status owner-tx) ::ACTIVE)
                  (vreset! (:status owner-tx) ::ABORTED))))
            (swap! ref assoc :owner thief)
            true)
          false))
      (finally
        (monitor-exit lock)))))

(defn- process-result [tx result]
  (cond
    (:ok result)
    (:ok result)

    (:retry result)
    (do
      (events/rollback! (:id tx))

      (let [n (.get ^AtomicInteger (:retry-count tx))]
        (when (pos? n)
          (Thread/sleep (bit-shift-left 1 (min n 5)))))

      (let [tx' (make-transaction)]
        (.set ^AtomicInteger (:retry-count tx')
              (.incrementAndGet ^AtomicInteger (:retry-count tx)))
        tx'))

    (:abort result)
    (do
      (events/rollback! (:id tx))
      (throw (:abort result)))))

(defn- commit [tx]
  (retry-if-aborted tx)

  (let [^HashMap rs (:read-set tx)
        ^HashMap ws (:write-set tx)]
    (when (seq ws)
      (locking COMMIT_LOCK
        (retry-if-aborted tx)
        (ensure-read-consistency rs)

        (apply-writes! ws)))

    (vreset! (:status tx) ::COMMITTED)
    (.remove ACTIVE_TRANSACTIONS (:id tx))
    (events/commit! (:id tx))))

(defn- run [tx fun]
  (loop [tx tx]
    (let [result
          (->> (try
                 (binding [*current-transaction* tx]
                   {:ok (let [r (fun)]
                          (commit tx)
                          r)})

                 (catch clojure.lang.ExceptionInfo e
                   (if (= (:type (ex-data e)) ::ABORTED)
                     {:retry true}
                     (throw e)))

                 (catch Throwable t
                   {:abort t}))
               (process-result tx))]
      (if (map? result)
        (recur result)
        result))))

(defn sync [fun]
  (if *current-transaction*
    (fun)
    (run (make-transaction) fun)))

(defmacro dosync [& body]
  `(sync (fn* [] ~@body)))

(defn ref [val]
  (let [write-point (.get WRITE_POINT)]
    (atom {:owner nil
           :write-point write-point
           :history [{:value val :write-point write-point}]
           :lock (Object.)})))

(defn deref [ref]
  (if-not *current-transaction*
    (:value (last (:history @ref)))
    (let [tx *current-transaction*]
      (retry-if-aborted tx)

      (let [^HashMap rs (:read-set tx)
            ^HashMap ws (:write-set tx)]
        (or (.get ws ref)
            (when-let [cached (.get rs ref)]
              (:value cached))

            (let [entry (find-version (:read-point tx) (:history @ref))]
              (when-not entry
                (retry))

              (.put rs ref {:value (:value entry)
                            :write-point (:write-point entry)})
              (:value entry)))))))

(defn ref-set [ref val]
  (when-not *current-transaction*
    (throw (IllegalStateException. "ref-set outside transaction")))

  (let [tx *current-transaction*]
    (retry-if-aborted tx)

    (when-not (acquire-ownership ref tx)
      (retry))

    (.put ^HashMap (:write-set tx) ref val)
    val))

(defn alter [ref fun & args]
  (ref-set ref (apply fun (deref ref) args)))

(defn on-rollback [fun]
  (let [tx *current-transaction*]
    (if tx
      (events/on-rollback (:id tx) fun)
      (throw (IllegalStateException. "on-rollback outside transaction")))))

(defn on-commit [fun]
  (let [tx *current-transaction*]
    (if tx
      (events/on-commit (:id tx) fun)
      (throw (IllegalStateException. "on-commit outside transaction")))))