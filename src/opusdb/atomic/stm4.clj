(ns opusdb.atomic.stm4
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
  (->> (eduction
        (filter some?)
        (filter #(<= (:write-point %) read-point))
        (rseq history))
       first))

(defn- try-claim-or-steal [ref thief-tx]
  (let [lock (:lock @ref)]
    (locking lock
      (let [thief-id (:id thief-tx)
            owner-id (:owner-id @ref)]
        (cond
          (or (nil? owner-id) (= owner-id thief-id))
          (do
            (swap! ref assoc :owner-id thief-id)
            true)

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
                    wp'))))]
      (commit* sorted-refs))

    (reset! (:status tx) ::COMMITTED)
    (swap! ACTIVE_TRANSACTIONS dissoc (:id tx))))

(defn- run [tx fun]
  (loop [tx' tx]
    (if-let [result (binding [*current-transaction* tx']
                      (try
                        (let [r (fun)]
                          (commit tx')
                          {:ok r})
                        (catch clojure.lang.ExceptionInfo e
                          (when-not (= (:type (ex-data e)) ::retry)
                            (throw e)))))]
      (:ok result)
      (recur (make-transaction)))))

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
  (if-not *current-transaction*
    (:value (last (:history @ref)))
    (let [tx *current-transaction*]
      (when (= @(:status tx) ::RETRY)
        (retry))
      (let [rs @(:read-set tx)
            ws @(:write-set tx)]
        (or (ws ref)
            (:value (rs ref))
            (let [ref' (find-before-or-at (:read-point tx) (:history @ref))]
              (when-not ref'
                (retry))
              (swap! (:read-set tx) assoc ref {:value (:value ref')
                                               :write-point (:write-point ref')})
              (:value ref')))))))

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