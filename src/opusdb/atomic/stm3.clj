(ns opusdb.atomic.stm3
  (:refer-clojure :exclude [ref deref ref-set alter dosync sync]))

(def ^:private MAX_HISTORY 10)
(def ^:private WRITE_POINT (atom 0))
(def ^:private TRANSACTION_ID (atom 0))
(def ^:private ACTIVE_TRANSACTIONS (atom {}))

(def ^{:private true :dynamic true} *current-transaction* nil)

(defn- make-transaction []
  (let [rp @WRITE_POINT
        tx-id (swap! TRANSACTION_ID inc)
        tx {:id tx-id
            :read-point rp
            :read-set (atom {})
            :write-set (atom {})
            :status (atom ::RUNNING)}]
    (swap! ACTIVE_TRANSACTIONS assoc tx-id tx)
    tx))

(defn- retry []
  (throw (ex-info "Transaction retry" {:type ::retry})))

(defn- find-before-or-at [read-point history]
  (loop [history' history]
    (let [ref' (peek history')]
      (cond
        (nil? ref') nil
        (<= (:write-point ref') read-point) ref'
        :else (recur (pop history'))))))

(defn- try-claim-or-steal [ref thief-tx]
  (let [lock (:lock @ref)]
    (locking lock
      (let [thief-id (:id thief-tx)
            owner-id (:owner-id @ref)]
        (cond
          (or (nil? owner-id) (= owner-id thief-id))
          (do (swap! ref assoc :owner-id thief-id) true)

          (> thief-id owner-id)
          (do
            (when-let [owner-tx (get @ACTIVE_TRANSACTIONS owner-id)]
              (when (= @(:status owner-tx) ::RUNNING)
                (reset! (:status owner-tx) ::RETRY)))
            (swap! ref assoc :owner-id thief-id)
            true)

          :else
          false)))))

(defn- commit [tx]
  (when (= @(:status tx) ::RETRY)
    (retry))

  (let [rs @(:read-set tx)
        ws @(:write-set tx)
        sorted-refs (sort-by hash (keys ws))]
    (letfn [(commit* [refs]
              (if (empty? refs)
                (do
                  (when (= @(:status tx) ::RETRY)
                    (retry))

                  (doseq [[ref _] rs]
                    (when (> (:write-point @ref) (:read-point tx))
                      (retry)))

                  (let [wp' (swap! WRITE_POINT inc)]
                    (doseq [[ref value] ws]
                      (swap! ref
                             (fn [ref-state]
                               (let [history' (conj (subvec (:history ref-state) 1)
                                                    {:value value
                                                     :write-point wp'})]
                                 (-> ref-state
                                     (assoc :write-point wp')
                                     (assoc :history history')
                                     (dissoc :owner-id))))))
                    wp'))

                (let [lock (:lock @(first refs))]
                  (locking lock
                    (commit* (rest refs))))))]
      (commit* sorted-refs))

    (reset! (:status tx) ::COMMITTED)
    (swap! ACTIVE_TRANSACTIONS dissoc (:id tx))))

(defn- run [tx fun]
  (loop [tx' tx]
    (let [result
          (binding [*current-transaction* tx']
            (try
              (let [r (fun)]
                (commit tx')
                {:ok r})
              (catch clojure.lang.ExceptionInfo e
                (if-not (= (:type (ex-data e)) ::retry)
                  (throw e)
                  nil))))]
      (if result
        (:ok result)
        (recur (make-transaction))))))

(defn sync [fun]
  (if *current-transaction*
    (fun)
    (run (make-transaction) fun)))

(defmacro dosync [& body]
  `(sync (fn* [] ~@body)))

(defn ref [val]
  (atom {:owner-id nil
         :write-point @WRITE_POINT
         :history (conj (vec (repeat (dec MAX_HISTORY) nil))
                        {:value val :write-point @WRITE_POINT})
         :lock (Object.)}))

(defn deref [ref]
  (if *current-transaction*
    (let [tx *current-transaction*]
      (when (= @(:status tx) ::RETRY)
        (retry))
      (let [rs @(:read-set tx)
            ws @(:write-set tx)]
        (cond
          (contains? ws ref) (get ws ref)
          (contains? rs ref) (get rs ref)
          :else
          (let [ref' (find-before-or-at (:read-point tx) (:history @ref))]
            (when (nil? ref')
              (retry))
            (swap! (:read-set tx) assoc ref (:value ref'))
            (:value ref')))))
    (:value (last (:history @ref)))))

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