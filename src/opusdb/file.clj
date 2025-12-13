(ns opusdb.file
  (:refer-clojure :exclude [*])
  (:require [opusdb.page]
            [opusdb.lru :as lru])
  (:import [java.io File]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file StandardOpenOption]
           [java.util LinkedHashMap]))

(def *
  (fn* [x y] (unchecked-multiply-int x y)))

(definterface IFileMgr
  (^int blockSize [])
  (^long fileSize [^String file-name])
  (^void read [^clojure.lang.IPersistentMap block ^opusdb.page.Page page])
  (^void write [^clojure.lang.IPersistentMap block ^opusdb.page.Page page])
  (^clojure.lang.IPersistentMap append [^String file-name]))

(defn- open-channel!
  [^LinkedHashMap channels db-dir file-name]
  (locking channels
    (or (.get channels file-name)
        (let [path (.toPath (File. (str db-dir "/" file-name)))
              channel (FileChannel/open
                       path
                       (into-array StandardOpenOption
                                   [StandardOpenOption/READ
                                    StandardOpenOption/WRITE
                                    StandardOpenOption/DSYNC
                                    StandardOpenOption/CREATE]))]
          (.put channels file-name channel)
          channel))))

(defrecord FileMgr [^LinkedHashMap channels
                    ^String db-dir
                    ^int block-size
                    strip-locks]

  IFileMgr

  (blockSize [_] block-size)

  (fileSize [_ file-name]
    (.length (File. (str db-dir "/" file-name))))

  (read [_ {:keys [file-name block-id] :as block} page]
    (try
      (let [^FileChannel channel (open-channel! channels db-dir file-name)
            lock (strip-locks (mod (hash file-name) (count strip-locks)))
            offset (* block-id block-size)
            ^ByteBuffer buffer (.rewind page)]
        (locking lock
          (.position channel ^long offset)
          (.read channel buffer)))
      (catch Exception e
        (throw (ex-info "Failed to read block"
                        {:block block :offset (* block-id block-size)}
                        e)))))

  (write [_ {:keys [file-name block-id] :as block} page]
    (try
      (let [^FileChannel channel (open-channel! channels db-dir file-name)
            lock (strip-locks (mod (hash file-name) (count strip-locks)))
            offset (* block-id block-size)
            ^ByteBuffer buffer (.rewind page)]
        (locking lock
          (.position channel ^long offset)
          (.write channel buffer)))
      (catch Exception e
        (throw (ex-info "Failed to write block"
                        {:block block :offset (* block-id block-size)}
                        e)))))

  (append [_ file-name]
    (try
      (let [^FileChannel channel (open-channel! channels db-dir file-name)
            lock (strip-locks (mod (hash file-name) (count strip-locks)))]
        (locking lock
          (let [block-id (quot (.size channel) block-size)
                offset (* block-id block-size)
                buffer (ByteBuffer/allocate block-size)]
            (.position channel ^long offset)
            (.write channel buffer)
            {:file-name file-name :block-id block-id})))
      (catch Exception e
        (throw (ex-info "Failed to append block"
                        {:file-name file-name}
                        e))))))

(defn- eviction-fn [{:keys [_ value]}]
  (.close ^FileChannel value)
  true)

(defn make-file-mgr
  ([db-dir block-size]
   (make-file-mgr db-dir block-size 100))
  ([db-dir block-size max-open-channels]
   (let [dir (File. ^String db-dir)
         new-db? (not (.exists dir))]
     (when new-db? (.mkdirs dir))
     (doseq [temp (eduction (filter #(.startsWith ^String % "temp"))
                            (.list dir))]
       (try
         (.delete (File. (str db-dir "/" temp)))
         (catch Exception _
           (println "Warning: Failed to delete temp file" temp))))
     (->FileMgr (lru/make-lru-cache max-open-channels eviction-fn)
                db-dir
                block-size
                (vec (repeatedly 16 #(Object.)))))))
