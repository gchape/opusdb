(ns opusdb.atomic.stm
  (:refer-clojure :exclude [ref deref ref-set alter dosync sync]))

(def COMMIT_LOCK (Object.))

(def ^:private MAX_HISTORY 8)
(def ^:private WRITE_POINT (atom 0))
(def ^:private HISTORY_TAIL (repeat (dec MAX_HISTORY) nil))

(def ^:dynamic *current-transaction* nil)

(defn- make-transaction []
  {:read-point @WRITE_POINT
   :read-set   (atom {})
   :write-set  (atom #{})
   :values     (atom {})})

(defn- retry []
  (throw (ex-info "Transaction aborted" {})))

(defn- find-before-or-at [read-pt history]
  (some #(when (<= (:write-point %) read-pt) %) history))

(defn- read* [ref]
  (let [tx *current-transaction*
        values @(:values tx)]
    (if (contains? values ref)
      (get values ref)
      (let [entry (find-before-or-at (:read-point tx) @ref)]
        (when-not entry
          (retry))
        (swap! (:read-set tx) assoc ref (:write-point entry))
        (swap! (:values tx) assoc ref (:value entry))
        (:value entry)))))

(defn- write* [ref val]
  (let [tx *current-transaction*]
    (swap! (:write-set tx) conj ref)
    (swap! (:values tx) assoc ref val)
    val))

(defn- commit []
  (let [tx *current-transaction*
        read-set  @(:read-set tx)
        write-set @(:write-set tx)]
    (locking COMMIT_LOCK
      ;; Phase 1: validate reads
      (doseq [[ref wp] read-set]
        (when-not (= (:write-point (first @ref)) wp)
          (retry)))
      ;; Phase 2: commit writes
      (when-not (empty? write-set)
        (let [values @(:values tx)
              wp (swap! WRITE_POINT inc)]
          (doseq [ref write-set]
            (swap! ref
                   (fn [history]
                     (cons {:value (get values ref)
                            :write-point wp}
                           (butlast history))))))))))

(defn- run [tx f]
  (let [result
        (binding [*current-transaction* tx]
          (try
            (let [r (f)]
              (commit)
              {:ok r})
            (catch clojure.lang.ExceptionInfo _
              nil)))]
    (if result
      (:ok result)
      (recur (make-transaction) f))))

(defn sync [f]
  (if *current-transaction*
    (f)
    (run (make-transaction) f)))

(defmacro dosync [& body]
  `(sync (fn* [] ~@body)))

(defn ref [val]
  (atom
   (cons {:value val
          :write-point @WRITE_POINT}
         HISTORY_TAIL)))

(defn deref [ref]
  (if *current-transaction*
    (read* ref)
    (:value (first @ref))))

(defn ref-set [ref val]
  (if *current-transaction*
    (write* ref val)
    (throw (IllegalStateException.
            "Cannot ref-set outside transaction"))))

(defn alter [ref f & args]
  (ref-set ref (apply f (deref ref) args)))