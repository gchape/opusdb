(ns opusdb.file
  (:refer-clojure :exclude [read])
  (:require [opusdb.page]
            [opusdb.lru :as l])
  (:import [java.io File]
           [opusdb.page Page]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file StandardOpenOption]
           [java.util LinkedHashMap]))

(defn open-channel!
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

(defn block-size
  [file-mgr]
  (:block-size file-mgr))

(defn file-size
  [file-mgr file-name]
  (.length (File. (str (:db-dir file-mgr) "/" file-name))))

(defn read
  [file-mgr {:keys [file-name index] :as block-id} ^Page page]
  (try
    (let [^FileChannel channel (open-channel! (:channels file-mgr) (:db-dir file-mgr) file-name)
          lock (nth (:strip-locks file-mgr) (mod (hash file-name) (count (:strip-locks file-mgr))))
          offset (* index (:block-size file-mgr))
          ^ByteBuffer buffer (.rewind page)]
      (locking lock
        (.position channel ^long offset)
        (.read channel buffer)))
    (catch Exception e
      (throw (ex-info "Failed to read block"
                      {:block-id block-id :offset (* index (:block-size file-mgr))}
                      e)))))

(defn write
  [file-mgr {:keys [file-name index] :as block-id} ^Page page]
  (try
    (let [^FileChannel channel (open-channel! (:channels file-mgr) (:db-dir file-mgr) file-name)
          lock (nth (:strip-locks file-mgr) (mod (hash file-name) (count (:strip-locks file-mgr))))
          offset (* index (:block-size file-mgr))
          ^ByteBuffer buffer (.rewind page)]
      (locking lock
        (.position channel ^long offset)
        (.write channel buffer)))
    (catch Exception e
      (throw (ex-info "Failed to write block"
                      {:block-id block-id :offset (* index (:block-size file-mgr))}
                      e)))))

(defn append
  [file-mgr file-name]
  (try
    (let [^FileChannel channel (open-channel! (:channels file-mgr) (:db-dir file-mgr) file-name)
          lock (nth (:strip-locks file-mgr) (mod (hash file-name) (count (:strip-locks file-mgr))))]
      (locking lock
        (let [index (quot (.size channel) (:block-size file-mgr))
              offset (* index (:block-size file-mgr))
              buffer (ByteBuffer/allocate (:block-size file-mgr))]
          (.position channel ^long offset)
          (.write channel buffer)
          {:file-name file-name :index index})))
    (catch Exception e
      (throw (ex-info "Failed to append block"
                      {:file-name file-name}
                      e)))))

(defn eviction-fn [{:keys [_ value]}]
  (.close ^FileChannel value)
  true)

(defrecord FileMgr [^LinkedHashMap channels
                    ^String db-dir
                    ^int block-size
                    ^LinkedHashMap strip-locks])

(defn make-file-mgr
  ([db-dir block-size]
   (make-file-mgr db-dir block-size 100))
  ([db-dir block-size max-open-channels]
   (let [dir (File. ^String db-dir)
         new? (not (.exists dir))]
     (when new? (.mkdirs dir))
     (doseq [temp (eduction (filter #(.startsWith ^String % "temp"))
                            (.list dir))]
       (try
         (.delete (File. (str db-dir "/" temp)))
         (catch Exception _
           (println "Warning: Failed to delete temp file" temp))))
     (->FileMgr (l/make-lru-cache max-open-channels eviction-fn)
                db-dir
                block-size
                (vec (repeatedly 16 #(Object.)))))))
