(ns opusdb.atomic.stm
  (:refer-clojure :exclude [ref deref ref-set alter dosync sync])
  (:import
   [java.util.concurrent ConcurrentHashMap]))

(def ^:private MAX_HISTORY 128)
(def ^:private WRITE_POINT (atom 0))
(def ^:private TRANSACTION_ID (atom 0))
(def ^:private ACTIVE_TRANSACTIONS (ConcurrentHashMap.))

(def ^{:private true :dynamic true} *current-transaction* nil)

(defn- make-tx []
  (let [read-point @WRITE_POINT
        tx-id (swap! TRANSACTION_ID inc)
        tx {:id tx-id
            :retries (atom 0)
            :read-set (atom {})
            :write-set (atom {})
            :read-point read-point
            :status (volatile! ::RUNNING)}]
    (.put ACTIVE_TRANSACTIONS tx-id tx)
    tx))

(defn- retry []
  (throw (ex-info "Transaction retry" {:type ::retry})))

(defn- find-at-or-before [read-point history]
  (loop [lo 0
         hi (dec (count history))
         result nil]
    (if (<= lo hi)
      (let [mid (quot (+ lo hi) 2)
            entry (nth history mid)
            write-point (:write-point entry)]
        (cond
          (= write-point read-point) entry
          (< write-point read-point) (recur (inc mid) hi entry)
          :else (recur lo (dec mid) result)))
      result)))

(defn- try-claim-or-steal [ref tx]
  (let [lock (:lock @ref)]
    (locking lock
      (let [thief (:id tx)
            owner (:owner @ref)]
        (cond
          (or (nil? owner) (= owner thief))
          (do
            (swap! ref assoc :owner thief)
            true)

          (> thief owner)
          (do
            (when-let [owner-tx (.get ACTIVE_TRANSACTIONS owner)]
              (when (= @(:status owner-tx) ::RUNNING)
                (vreset! (:status owner-tx) ::RETRY)))
            (swap! ref assoc :owner thief)
            true)

          :else
          false)))))

(defn- commit [tx]
  (when (= @(:status tx) ::RETRY)
    (retry))

  (let [rs @(:read-set tx)
        ws @(:write-set tx)]
    (letfn [(commit* [refs]
              (if (seq refs)
                (let [lock (:lock @(first refs))]
                  (locking lock
                    (commit* (rest refs))))
                (do
                  (when (= @(:status tx) ::RETRY)
                    (retry))

                  (doseq [[ref {:keys [write-point]}] rs]
                    (when (> (:write-point @ref) write-point)
                      (retry)))

                  (let [write-point' (swap! WRITE_POINT inc)]
                    (doseq [[ref val] ws]
                      (swap! ref
                             (fn [state]
                               (let [h (conj (:history state) {:value val :write-point write-point'})
                                     h' (if (> (count h) MAX_HISTORY)
                                          (subvec h 1)
                                          h)]
                                 (-> state
                                     (dissoc :owner)
                                     (assoc :history h')
                                     (assoc :write-point write-point'))))))
                    write-point'))))]
      (commit* (sort-by hash (keys ws))))

    (vreset! (:status tx) ::COMMITTED)
    (.remove ACTIVE_TRANSACTIONS (:id tx))))

(defn- run [tx fun]
  (loop [tx' tx]
    (if-let [result (binding [*current-transaction* tx']
                      (try
                        (let [r (fun)]
                          (commit tx')
                          {:ok r})
                        (catch clojure.lang.ExceptionInfo e
                          (if (= (:type (ex-data e)) ::retry)
                            (do
                              (swap! (:retries tx') inc)
                              nil)
                            (do
                              (.remove ACTIVE_TRANSACTIONS (:id tx'))
                              (throw e))))
                        (catch Throwable t
                          (.remove ACTIVE_TRANSACTIONS (:id tx'))
                          (throw t))))]
      (:ok result)
      (let [r @(:retries tx')
            base (bit-shift-left 1 (min r 6))
            limit (min 64 base)
            ms (rand-int (inc limit))]
        (when (pos? ms)
          (Thread/sleep ms))
        (recur (make-tx))))))

(defn sync [fun]
  (if *current-transaction*
    (fun)
    (run (make-tx) fun)))

(defmacro dosync [& body]
  `(sync (fn* [] ~@body)))

(defn ref [val]
  (let [wp @WRITE_POINT]
    (atom {:owner nil
           :write-point wp
           :history [{:value val :write-point wp}]
           :lock (Object.)})))

(defn deref [ref]
  (if-not *current-transaction*
    (:value (last (:history @ref)))
    (let [tx *current-transaction*]
      (when (= @(:status tx) ::RETRY)
        (retry))
      (let [reads @(:read-set tx)
            writes @(:write-set tx)]
        (or (writes ref)
            (:value (reads ref))
            (let [entry (find-at-or-before (:read-point tx) (:history @ref))]
              (when-not entry
                (retry))

              (swap! (:read-set tx) assoc ref {:value (:value entry)
                                               :write-point (:write-point entry)})
              (:value entry)))))))

(defn ref-set [ref val]
  (when-not *current-transaction*
    (throw (IllegalStateException. "ref-set outside transaction")))

  (let [tx *current-transaction*]
    (when (= @(:status tx) ::RETRY)
      (retry))

    (when-not (try-claim-or-steal ref tx)
      (retry))

    (swap! (:write-set tx) assoc ref val)
    val))

(defn alter [ref fun & args]
  (ref-set ref (apply fun (deref ref) args)))