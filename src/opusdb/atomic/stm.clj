(ns opusdb.atomic.stm
  (:refer-clojure :exclude [ref deref ref-set alter dosync sync]))

(def ^:private MAX_HISTORY 8)
(def ^:private WRITE_POINT (atom 0))
(def ^:private GLOBAL_LOCK (Object.))
(def ^:private INIT_HISTORY (into [] (repeat (dec MAX_HISTORY) nil)))

(def ^:dynamic *current-transaction* nil)

(defn- make-transaction []
  {:read-point @WRITE_POINT
   :read-set   (atom {})
   :write-set  (atom {})})

(defn- retry []
  (throw (ex-info "Transaction aborted" {})))

(defn- find-before-or-at [read-point history]
  (->> (rseq history)
       (filterv #(and % (<= (:write-point %) read-point)))
       (first)))

(defn- read* [ref]
  (let [tx *current-transaction*
        rs @(:read-set tx)
        ws @(:write-set tx)]
    (or (ws ref)
        (:value (rs ref))
        (let [entry (find-before-or-at (:read-point tx) (:history @ref))]
          (when-not entry
            (retry))
          (swap! (:read-set tx) assoc ref {:write-point (:write-point entry)
                                           :value (:value entry)})
          (:value entry)))))

(defn- write* [ref val]
  (swap! (:write-set *current-transaction*) assoc ref val)
  val)

(defn- commit []
  (let [tx *current-transaction*
        rs @(:read-set tx)
        ws @(:write-set tx)]

    (locking GLOBAL_LOCK
      (doseq [[ref {:keys [write-point]}] rs]
        (when-not (= (:write-point (last (:history @ref))) write-point)
          (retry)))

      (when (seq ws)
        (let [write-point' (swap! WRITE_POINT inc)]
          (doseq [[ref value] ws]
            (swap! ref update :history
                   #(conj (subvec % 1) {:value value
                                        :write-point write-point'})))
          write-point')))))

(defn- run [tx fun]
  (if-let [result (binding [*current-transaction* tx]
                    (try
                      (let [r (fun)]
                        (commit)
                        {:ok r})
                      (catch clojure.lang.ExceptionInfo _
                        nil)))]
    (:ok result)
    (recur (make-transaction) fun)))

(defn sync [fun]
  (if *current-transaction*
    (fun)
    (run (make-transaction) fun)))

(defmacro dosync [& body]
  `(sync (fn* [] ~@body)))

(defn ref [val]
  (atom
   {:history
    (conj INIT_HISTORY
          {:value val :write-point @WRITE_POINT})}))

(defn deref [ref]
  (if *current-transaction*
    (read* ref)
    (:value (last (:history @ref)))))

(defn ref-set [ref val]
  (if *current-transaction*
    (write* ref val)
    (throw (IllegalStateException.
            "Cannot ref-set outside transaction"))))

(defn alter [ref fun & args]
  (ref-set ref (apply fun (deref ref) args)))