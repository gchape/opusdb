(ns opusdb.atomic.stm
  (:refer-clojure :exclude [ref deref ref-set alter dosync sync])
  (:require
   [opusdb.atomic.tx-hooks :as tx])
  (:import
   [java.util Collections IdentityHashMap]
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
        tx-id      (.incrementAndGet TRANSACTION_ID)
        tx         {:id          tx-id
                    :read-set    (IdentityHashMap.)
                    :write-set   (IdentityHashMap.)
                    :read-point  read-point
                    :retry-count (AtomicInteger.)
                    :status      (volatile! ::ACTIVE)}]
    (.put ACTIVE_TRANSACTIONS tx-id tx)
    tx))

(defn- next-transaction [tx]
  ;; flush rollback handlers before creating a fresh transaction
  (tx/rollback! (:id tx))
  (let [n (.get ^AtomicInteger (:retry-count tx))]
    ;; exponential backoff after the first retry
    (when (pos? n)
      (Thread/sleep (bit-shift-left 1 (min n 5)))))
  (let [tx' (make-transaction)]
    (.set ^AtomicInteger (:retry-count tx')
          (.incrementAndGet ^AtomicInteger (:retry-count tx)))
    tx'))

(defn- retry []
  (throw (ex-info "Transaction retry" {:type ::ABORTED})))

(defn- retry-if-aborted [tx]
  (when (= @(:status tx) ::ABORTED)
    (retry)))

(defn- find-entry [read-point history]
  (let [index (Collections/binarySearch history
                                        read-point
                                        #(compare (:write-point %1) %2))]
    (if (>= index 0)
      (history index)
      (let [index' (- (inc index))]
        (when (pos? index')
          (history (dec index')))))))

(defn- ensure-read-consistency [read-set]
  ;; abort if any ref was written after this transaction's read point
  (doseq [[ref val] read-set]
    (when (> (:write-point @ref) (:write-point val))
      (retry))))

(defn- apply-writes! [write-set]
  (let [write-point' (.incrementAndGet WRITE_POINT)]
    (doseq [[ref val] write-set]
      (swap! ref
             (fn [{:keys [history] :as state}]
               (let [history' (conj history {:value val :write-point write-point'})
                     ;; trim to bounded history window; subvec is O(1)
                     history' (cond-> history'
                                (> (count history') MAX_HISTORY)
                                (subvec (- (count history') MAX_HISTORY)))]
                 (assoc state
                        :value       val
                        :history     history'
                        :write-point write-point'
                        :owner       nil)))))))

(defn- commit [tx]
  ;; check before acquiring the lock to avoid unnecessary contention
  (retry-if-aborted tx)
  (let [^IdentityHashMap rs (:read-set tx)
        ^IdentityHashMap ws (:write-set tx)]
    (when (seq ws)
      (locking COMMIT_LOCK
        ;; re-check after acquiring the lock; may have been aborted by a thief
        (retry-if-aborted tx)
        (ensure-read-consistency rs)
        (apply-writes! ws)))
    (vreset! (:status tx) ::COMMITTED)
    (.remove ACTIVE_TRANSACTIONS (:id tx))
    (tx/commit! (:id tx))))

(defn- execute [tx fun]
  (try
    (binding [*current-transaction* tx]
      (let [r (fun)]
        (commit tx)
        [:ok r]))
    (catch clojure.lang.ExceptionInfo e
      ;; distinguish stm abort signals from real user exceptions
      (if (= (:type (ex-data e)) ::ABORTED)
        [:retry nil]
        (throw e)))
    (catch Exception e
      [:abort e])))

(defn- run [tx fun]
  (loop [tx tx]
    (let [[outcome value] (execute tx fun)]
      (case outcome
        :ok    value
        :retry (recur (next-transaction tx))
        :abort (do (tx/rollback! (:id tx))
                   (throw value))))))

(defn- acquire-ownership [ref tx]
  ;; per-ref lock ensures atomic owner check-and-set
  (let [lock (:lock @ref)]
    (monitor-enter lock)
    (try
      (let [thief (:id tx)
            owner (:owner @ref)]
        (if (or (nil? owner) (>= thief owner))
          (do
            ;; higher tx-id steals ownership and aborts the lower transaction
            (when (and owner (> thief owner))
              (when-some [owner-tx (.get ACTIVE_TRANSACTIONS owner)]
                (when (= @(:status owner-tx) ::ACTIVE)
                  (vreset! (:status owner-tx) ::ABORTED))))
            (swap! ref assoc :owner thief)
            true)
          false))
      (finally
        (monitor-exit lock)))))

(defn sync [fun]
  ;; nested dosync calls reuse the ambient transaction
  (if *current-transaction*
    (fun)
    (run (make-transaction) fun)))

(defmacro dosync [& body]
  `(sync (fn* [] ~@body)))

(defn ref [val]
  (let [write-point (.get WRITE_POINT)]
    (atom {:owner       nil
           :value       val
           :write-point write-point
           :history     [{:value val :write-point write-point}]
           :lock        (Object.)})))

(defn deref [ref]
  (if-not *current-transaction*
    ;; fast path: no transaction, read the latest committed value directly
    (:value @ref)
    (let [tx *current-transaction*]
      (retry-if-aborted tx)
      (let [^IdentityHashMap rs (:read-set tx)
            ^IdentityHashMap ws (:write-set tx)]
        (or (.get ws ref)
            (when-let [cached (.get rs ref)]
              (:value cached))
            (let [entry (find-entry (:read-point tx) (:history @ref))]
              (when-not entry
                (retry))
              (.put rs ref {:value       (:value entry)
                            :write-point (:write-point entry)})
              (:value entry)))))))

(defn ref-set [ref val]
  (when-not *current-transaction*
    (throw (IllegalStateException. "ref-set outside transaction")))
  (let [tx *current-transaction*]
    (retry-if-aborted tx)
    (when-not (acquire-ownership ref tx)
      (retry))
    (.put ^IdentityHashMap (:write-set tx) ref val)
    val))

(defn alter [ref fun & args]
  (ref-set ref (apply fun (deref ref) args)))

(defn on-rollback [fun]
  (if-let [tx *current-transaction*]
    (tx/on-rollback (:id tx) fun)
    (throw (IllegalStateException. "on-rollback outside transaction"))))

(defn on-commit [fun]
  (if-let [tx *current-transaction*]
    (tx/on-commit (:id tx) fun)
    (throw (IllegalStateException. "on-commit outside transaction"))))