(ns opusdb.atomic.stm
  (:refer-clojure :exclude [ref deref ref-set alter dosync sync]))

(def ^:private MAX_HISTORY 8)
(def ^:private WRITE_POINT (atom 0))
(def ^:private HISTORY_TAIL (repeat (dec MAX_HISTORY) nil))

(def ^:dynamic *current-transaction* nil)

(defn- make-transaction []
  {:read-point @WRITE_POINT
   :read-set   (atom {})      ;; {ref -> {:wp write-point :val value}}
   :write-set  (atom {})})    ;; {ref -> value}

(defn- retry []
  (throw (ex-info "Transaction aborted" {})))

(defn- find-before-or-at [read-pt history]
  (some #(when (<= (:wp %) read-pt) %) history))

(defn- read* [ref]
  (let [tx *current-transaction*
        rs @(:read-set tx)
        ws @(:write-set tx)]
    ;; read-your-own-writes
    (if (contains? ws ref)
      (ws ref)
      ;; Check if already read
      (if (contains? rs ref)
        (:val (rs ref))  ; return cached value from read-set
        ;; First read - find appropriate version
        (let [entry (find-before-or-at (:read-point tx) (:history @ref))]
          (when-not entry
            (retry))
          ;; Store both value and write-point for validation
          (swap! (:read-set tx) assoc ref {:wp (:wp entry)
                                           :val (:val entry)})
          (:val entry))))))

(defn- write* [ref val]
  (swap! (:write-set *current-transaction*) assoc ref val)
  val)

(defn- commit []
  (let [tx *current-transaction*
        rs @(:read-set tx)
        ws @(:write-set tx)
        sorted-refs (sort-by hash (keys ws))]

    ;; If no writes, just validate read-set
    (when (empty? sorted-refs)
      (doseq [[ref {:keys [wp]}] rs]
        (when-not (= (:wp (first (:history @ref))) wp)
          (retry))))

    (letfn [(commit* [refs]
              (if (empty? refs)
                (do
                  ;; Validate read-set
                  (doseq [[ref {:keys [wp]}] rs]
                    (when-not (= (:wp (first (:history @ref))) wp)
                      (retry)))

                  ;; Validate write-set
                  (doseq [ref sorted-refs]
                    (let [current-wp (:wp (first (:history @ref)))]
                      (when (> current-wp (:read-point tx))
                        (retry))))

                  ;; All validation passed - assign write-point and commit
                  (let [wver (swap! WRITE_POINT inc)]
                    (doseq [ref sorted-refs]
                      (swap! ref update :history
                             #(cons {:val (ws ref)
                                     :wp wver}
                                    (butlast %))))
                    wver))

                ;; Acquire lock and recurse
                (let [lock (:lock @(first refs))]
                  (locking lock
                    (commit* (rest refs))))))]
      (commit* sorted-refs))))

(defn- run [tx fun]
  (let [result
        (binding [*current-transaction* tx]
          (try
            (let [r (fun)]
              (commit)
              {:ok r})
            (catch clojure.lang.ExceptionInfo _
              nil)))]
    (if result
      (:ok result)
      (recur (make-transaction) fun))))

(defn sync [fun]
  (if *current-transaction*
    (fun)
    (run (make-transaction) fun)))

(defmacro dosync [& body]
  `(sync (fn* [] ~@body)))

(defn ref [val]
  (atom
   {:history
    (cons {:val val
           :wp @WRITE_POINT} HISTORY_TAIL)
    :lock (Object.)}))

(defn deref [ref]
  (if *current-transaction*
    (read* ref)
    (:val (first (:history @ref)))))

(defn ref-set [ref val]
  (if *current-transaction*
    (write* ref val)
    (throw (IllegalStateException.
            "Cannot ref-set outside transaction"))))

(defn alter [ref fun & args]
  (ref-set ref (apply fun (deref ref) args)))