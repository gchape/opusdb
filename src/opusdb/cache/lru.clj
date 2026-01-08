(ns opusdb.cache.lru
  (:import
   [java.util LinkedHashMap]))

(defn make-lru-cache
  ([capacity]
   (make-lru-cache capacity nil))
  ([capacity eviction-fn]
   (proxy [LinkedHashMap] [capacity 0.75 true]
     (removeEldestEntry [^java.util.Map$Entry entry]
       (let [^LinkedHashMap this this]
         (if (> (.size this) capacity)
           (if (not (nil? eviction-fn))
             (let [entry-map {:key (.getKey entry) :value (.getValue entry)}]
               (eviction-fn this entry-map))
             true)
           false))))))
