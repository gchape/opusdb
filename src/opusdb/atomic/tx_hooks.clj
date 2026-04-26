(ns opusdb.atomic.tx-hooks
  (:import
   [java.util.concurrent ConcurrentHashMap]))

(def ^:private ^ConcurrentHashMap EVENT_REGISTRAR (ConcurrentHashMap.))

(defn on-commit [id fn]
  (.compute EVENT_REGISTRAR (keyword "commit" (str id))
            (fn* [_ fns]
              (conj (or fns []) fn))))

(defn on-rollback [id fn]
  (.compute EVENT_REGISTRAR (keyword "rollback" (str id))
            (fn* [_ fns]
              (conj (or fns []) fn))))

(defn commit! [id]
  (when-let [fns (.remove EVENT_REGISTRAR (keyword "commit" (str id)))]
    (doseq [fn fns]
      (fn))))

(defn rollback! [id]
  (when-let [fns (.remove EVENT_REGISTRAR (keyword "rollback" (str id)))]
    (doseq [fn fns]
      (fn))))