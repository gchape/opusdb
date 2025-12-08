(ns opusdb.lru
  (:import [java.util LinkedHashMap]))

(defn make-lru
  ([size]
   (make-lru size nil))
  ([size eviction-fn]
   (let [lock (Object.)]
     (proxy [LinkedHashMap] [size 0.75 true]
       (removeEldestEntry [^java.util.Map$Entry entry]
         (if (> (.size this) size)
           (if (nil? eviction-fn)
             true
             (try
               (eviction-fn (.getKey entry) (.getValue entry))
               (catch Exception e
                 (println "Error during eviction:" (.getMessage e)))))
           false))
       (get [key]
         (locking lock
           (proxy-super get key)))
       (put [key value]
         (locking lock
           (proxy-super put key value)))
       (remove [key]
         (locking lock
           (proxy-super remove key)))
       (containsKey [key]
         (locking lock
           (proxy-super containsKey key)))
       (clear []
         (locking lock
           (proxy-super clear)))))))
