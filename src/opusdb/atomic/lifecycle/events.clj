(ns opusdb.atomic.lifecycle.events
  (:import
   [java.util.concurrent ConcurrentHashMap]))

(def ^:private ^ConcurrentHashMap COMMIT_EVENTS (ConcurrentHashMap.))
(def ^:private ^ConcurrentHashMap ROLLBACK_EVENTS (ConcurrentHashMap.))

(defn on-commit [id fun]
  (.compute COMMIT_EVENTS id
            (fn [_ fns]
              (conj (or fns []) fun))))

(defn on-rollback [id fun]
  (.compute ROLLBACK_EVENTS id
            (fn [_ fns]
              (conj (or fns []) fun))))

(defn commit! [id]
  (when-let [fns (.remove COMMIT_EVENTS id)]
    (doseq [f fns]
      (f))
    (.remove ROLLBACK_EVENTS id)))

(defn rollback! [id]
  (when-let [fns (.remove ROLLBACK_EVENTS id)]
    (doseq [f fns]
      (f))))