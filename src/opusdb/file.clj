(ns opusdb.file
  (:require [opusdb.lru :as lru])
  (:import [java.nio.file Files Path StandardOpenOption]
           [java.nio.channels FileChannel]
           [java.nio ByteBuffer]
           [java.util LinkedHashMap]))

(definterface IFileMngr
  (^int blockSize [])
  (^void readBlock [b p])
  (^void writeBlock [b p])
  (^clojure.lang.IPersistentMap append [filename]))

(defn- close-channel [_key channel]
  (try
    (.close ^FileChannel channel)
    (catch Exception e
      (println "Warning: Failed to close channel:" (.getMessage e)))))

(defn- get-or-open-channel
  [^LinkedHashMap cache ^String db-path ^String file-name]
  (locking cache
    (or (.get cache file-name)
        (let [^Path path (Path/of db-path (into-array String [file-name]))
              ^FileChannel channel (FileChannel/open path
                                                     (into-array StandardOpenOption
                                                                 [StandardOpenOption/READ
                                                                  StandardOpenOption/WRITE
                                                                  StandardOpenOption/DSYNC
                                                                  StandardOpenOption/CREATE]))]
          (.put cache file-name channel)
          channel))))

(defrecord FileMngr [^LinkedHashMap channelCache
                     ^String dbPath
                     ^int blockSize]
  IFileMngr
  (blockSize [_] blockSize)
  (readBlock [_ b p]
    (try
      (let [file-name (:file-name b)
            ch (get-or-open-channel channelCache dbPath file-name)
            offset (* (:block-id b) blockSize)
            buffer (.rewind p)]
        (locking ch
          (.position ch offset)
          (.read ch buffer)))
      (catch Exception e
        (throw (ex-info "Failed to read block"
                        {:block b
                         :file-name (:file-name b)
                         :block-id (:block-id b)
                         :offset (* (:block-id b) blockSize)}
                        e)))))
  (writeBlock [_ b p]
    (try
      (let [file-name (:file-name b)
            ch (get-or-open-channel channelCache dbPath file-name)
            offset (* (:block-id b) blockSize)
            buffer (.rewind p)]
        (locking ch
          (.position ch offset)
          (.write ch buffer)))
      (catch Exception e
        (throw (ex-info "Failed to write block"
                        {:block b
                         :file-name (:file-name b)
                         :block-id (:block-id b)
                         :offset (* (:block-id b) blockSize)}
                        e)))))

  (append [_ filename]
    (try
      (let [ch (get-or-open-channel channelCache dbPath filename)
            new-block-num (quot (.size ch) blockSize)
            offset (* new-block-num blockSize)
            buffer (ByteBuffer/allocate blockSize)]
        (locking ch
          (.position ch offset)
          (.write ch buffer))
        {:file-name filename :block-id new-block-num})
      (catch Exception e
        (throw (ex-info "Failed to append block"
                        {:file-name filename}
                        e))))))

(defn make-file-mngr
  ([db-path-str block-size]
   (make-file-mngr db-path-str block-size 100))
  ([db-path-str block-size max-open-files]
   (let [db-path (Path/of db-path-str (into-array String []))
         is-new? (not (Files/exists db-path (into-array java.nio.file.LinkOption [])))]
     (when is-new?
       (Files/createDirectory db-path (into-array java.nio.file.attribute.FileAttribute [])))
     (with-open [stream (Files/list db-path)]
       (doseq [path (stream-seq! stream)]
         (when (.startsWith (.toString (.getFileName path)) "temp")
           (try
             (Files/delete path)
             (catch Exception e
               (println "Warning: Failed to delete temp file" path ":" (.getMessage e)))))))
     (->FileMngr (lru/make-lru max-open-files close-channel)
                 db-path-str
                 block-size))))
