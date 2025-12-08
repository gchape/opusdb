(ns opusdb.lru
  (:import [java.util LinkedHashMap]))

(defn make-lru-cache
  ([size]
   (make-lru-cache size nil))
  ([size eviction-fn]
   (let [lock (Object.)]
     (proxy [LinkedHashMap] [size 0.75 true]
       (removeEldestEntry [^java.util.Map$Entry entry]
         (let [^LinkedHashMap this this]
           (if (> (.size this) size)
             (if (not (nil? eviction-fn))
               (try
                 (eviction-fn (.getKey entry) (.getValue entry))
                 (catch Exception e
                   (println "Error during eviction:" (.getMessage e))))
               true)
             false)))
       (get [key]
         (locking lock
           (let [^LinkedHashMap this this]
             (proxy-super get key))))
       (put [key value]
         (locking lock
           (let [^LinkedHashMap this this]
             (proxy-super put key value))))
       (remove [key]
         (locking lock
           (let [^LinkedHashMap this this]
             (proxy-super remove key))))
       (containsKey [key]
         (locking lock
           (let [^LinkedHashMap this this]
             (proxy-super containsKey key))))
       (clear []
         (locking lock
           (let [^LinkedHashMap this this]
             (proxy-super clear))))))))
