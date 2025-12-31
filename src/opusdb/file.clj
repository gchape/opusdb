(ns opusdb.file
  (:refer-clojure :exclude [read])
  (:require [opusdb.cache :as c]
            [opusdb.page :as p])
  (:import [java.io File]
           [java.nio ByteBuffer]
           [io.netty.buffer ByteBuf]
           [java.nio.channels FileChannel]
           [java.nio.file StandardOpenOption]
           [java.util LinkedHashMap]))

(defn block-size
  [file-mgr]
  (:block-size file-mgr))

(defn file-size
  [file-mgr file-name]
  (.length (File. (str (:db-dir file-mgr) "/" file-name))))

(defn- open-channel
  [cache-id db-dir file-name]
  (locking cache-id
    (or (c/get cache-id file-name)
        (let [path (.toPath (File. (str db-dir "/" file-name)))
              channel (FileChannel/open
                       path
                       (into-array StandardOpenOption
                                   [StandardOpenOption/READ
                                    StandardOpenOption/WRITE
                                    StandardOpenOption/DSYNC
                                    StandardOpenOption/CREATE]))]
          (c/put cache-id file-name channel)
          channel))))

(defn read
  [file-mgr {:keys [file-name index] :as block-id} ^ByteBuf page]
  (try
    (let [^FileChannel channel (open-channel (:cache-id file-mgr) (:db-dir file-mgr) file-name)
          lock (nth (:strip-locks file-mgr) (mod (hash file-name) (count (:strip-locks file-mgr))))
          size (block-size file-mgr)
          ^long offset (* index size)
          ^ByteBuffer buf (.nioBuffer page 0 size)]
      (locking lock
        (.position channel offset)
        (.read channel buf)
        (.setBytes page 0 buf)))
    (catch Exception e
      (throw (ex-info "Failed to read block"
                      {:block-id block-id}
                      e)))))

(defn write
  [file-mgr {:keys [file-name index] :as block-id} ^ByteBuf page]
  (try
    (let [^FileChannel channel (open-channel (:cache-id file-mgr) (:db-dir file-mgr) file-name)
          lock (nth (:strip-locks file-mgr)
                    (mod (hash file-name) (count (:strip-locks file-mgr))))
          block-size (:block-size file-mgr)
          ^long offset (* index block-size)
          ^ByteBuffer buf (.nioBuffer page 0 block-size)]
      (locking lock
        (.position channel offset)
        (.write channel buf)))
    (catch Exception e
      (throw (ex-info "Failed to write block"
                      {:block-id block-id}
                      e)))))

(defn append
  [file-mgr file-name]
  (try
    (let [^FileChannel channel (open-channel (:cache-id file-mgr) (:db-dir file-mgr) file-name)
          lock (nth (:strip-locks file-mgr)
                    (mod (hash file-name) (count (:strip-locks file-mgr))))]
      (locking lock
        (let [block-size (:block-size file-mgr)
              index (quot (.size channel) block-size)
              offset (* index block-size)
              ^ByteBuf page (p/make-page block-size)
              buf (.nioBuffer page 0 block-size)]
          (.position channel ^long offset)
          (.write channel buf)
          {:file-name file-name :index index})))
    (catch Exception e
      (throw (ex-info "Failed to append block"
                      {:file-name file-name}
                      e)))))

(defn eviction-fn [{:keys [_ value]}]
  (.close ^FileChannel value)
  true)

(defrecord FileMgr [^int cache-id
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
     (->FileMgr (c/make-cache max-open-channels eviction-fn)
                db-dir
                block-size
                (vec (repeatedly 16 #(Object.)))))))
