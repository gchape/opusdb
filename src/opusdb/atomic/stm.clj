(ns opusdb.atomic.stm
  (:refer-clojure :exclude [ref deref ref-set alter dosync sync])
  (:import
   [java.util.concurrent ConcurrentHashMap]))

(def ^:private TX_ID (atom 0))
(def ^:private MAX_HISTORY 128)
(def ^:private WRITE_POINT (atom 0))
(def ^:private ACTIVE_TX_MAP (ConcurrentHashMap.))

(def ^{:private true :dynamic true} *current-tx* nil)

(defn- make-tx []
  (let [rp @WRITE_POINT
        id (swap! TX_ID inc)
        tx {:id id
            :read-point rp
            :retries (atom 0)
            :read-set (atom {})
            :write-set (atom {})
            :status (volatile! ::RUNNING)}]
    (.put ACTIVE_TX_MAP id tx)
    tx))

(defn- retry []
  (throw (ex-info "Transaction retry" {:type ::retry})))

(defn- find-at-or-before [rp history]
  (loop [lo 0
         hi (dec (count history))
         result nil]
    (if (<= lo hi)
      (let [mid (quot (+ lo hi) 2)
            entry (nth history mid)
            wp (:write-point entry)]
        (cond
          (= wp rp) entry
          (< wp rp) (recur (inc mid) hi entry)
          :else (recur lo (dec mid) result)))
      result)))

(defn- claim-or-steal [r tx]
  (let [lock (:lock @r)]
    (locking lock
      (let [thief (:id tx)
            owner (:owner @r)]
        (cond
          (or (nil? owner) (= owner thief))
          (do
            (swap! r assoc :owner thief)
            true)

          (> thief owner)
          (do
            (when-let [owner-tx (.get ACTIVE_TX_MAP owner)]
              (when (= @(:status owner-tx) ::RUNNING)
                (vreset! (:status owner-tx) ::RETRY)))
            (swap! r assoc :owner thief)
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
                    (doseq [[ref val] ws]
                      (swap! ref
                             (fn [state]
                               (let [h (conj (:history state) {:value val :write-point wp'})
                                     h' (if (> (count h) MAX_HISTORY)
                                          (subvec h 1)
                                          h)]
                                 (-> state
                                     (assoc :write-point wp')
                                     (assoc :history h')
                                     (dissoc :owner))))))
                    wp'))))]
      (commit* sorted-refs))

    (vreset! (:status tx) ::COMMITTED)
    (.remove ACTIVE_TX_MAP (:id tx))))

(defn- run [tx f]
  (loop [tx tx]
    (if-let [result (binding [*current-tx* tx]
                      (try
                        (let [r (f)]
                          (commit tx)
                          {:ok r})
                        (catch clojure.lang.ExceptionInfo e
                          (if (= (:type (ex-data e)) ::retry)
                            (do
                              (swap! (:retries tx) inc)
                              nil)
                            (do
                              (.remove ACTIVE_TX_MAP (:id tx))
                              (throw e))))
                        (catch Throwable t
                          (.remove ACTIVE_TX_MAP (:id tx))
                          (throw t))))]
      (:ok result)
      (let [n @(:retries tx)
            ms (min 50 (bit-shift-left 1 (min n 5)))]
        (when (pos? ms)
          (Thread/sleep ms))
        (recur (make-tx))))))

(defn sync [f]
  (if *current-tx*
    (f)
    (run (make-tx) f)))

(defmacro dosync [& body]
  `(sync (fn* [] ~@body)))

(defn ref [val]
  (let [wp @WRITE_POINT]
    (atom {:owner nil
           :write-point wp
           :history [{:value val :write-point wp}]
           :lock (Object.)})))

(defn deref [r]
  (if-not *current-tx*
    (:value (last (:history @r)))
    (let [tx *current-tx*]
      (when (= @(:status tx) ::RETRY)
        (retry))
      (let [reads @(:read-set tx)
            writes @(:write-set tx)]
        (or (writes r)
            (:value (reads r))
            (let [entry (find-at-or-before (:read-point tx) (:history @r))]
              (when-not entry
                (retry))
              (swap! (:read-set tx) assoc r {:value (:value entry)
                                             :write-point (:write-point entry)})
              (:value entry)))))))

(defn ref-set [r val]
  (when-not *current-tx*
    (throw (IllegalStateException. "ref-set outside transaction")))

  (let [tx *current-tx*]
    (when (= @(:status tx) ::RETRY)
      (retry))
    (when-not (claim-or-steal r tx)
      (retry))
    (swap! (:write-set tx) assoc r val)
    val))

(defn alter [r f & args]
  (ref-set r (apply f (deref r) args)))