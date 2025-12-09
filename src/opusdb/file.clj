(ns opusdb.file
  (:require [opusdb.lru :as lru]
            [clojure.spec.alpha :as spec])
  (:import [java.io File]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file StandardOpenOption]
           [java.util LinkedHashMap]))

(spec/def ::file-name string?)
(spec/def ::blockn int?)
(spec/def ::block (spec/keys :req-un [::file-name ::blockn]))

(defn- block? [b]
  (when-not (spec/valid? ::block b)
    (throw (ex-info "Invalid block map"
                    {:block b}
                    (spec/explain-data ::block b)))))

(definterface IFileMngr
  (^int blockSize [])
  (^void readFrom [^clojure.lang.IPersistentMap b p])
  (^void writeTo [^clojure.lang.IPersistentMap b p])
  (^clojure.lang.IPersistentMap appendNew [^String fileName]))

(defn- get-or-open-channel
  [lru-cache db-path file-name]
  (locking lru-cache
    (or (.get ^LinkedHashMap lru-cache file-name)
        (let [path (.toPath (File. (str db-path "/" file-name)))
              ch (FileChannel/open path (into-array StandardOpenOption
                                                    [StandardOpenOption/READ
                                                     StandardOpenOption/WRITE
                                                     StandardOpenOption/DSYNC
                                                     StandardOpenOption/CREATE]))]
          (.put ^LinkedHashMap lru-cache file-name ch)
          ch))))

(defrecord FileMngr [^LinkedHashMap openChannels
                     ^String dbPath
                     ^int blockSize]
  IFileMngr
  (blockSize [_] blockSize)

  (readFrom [_ b p]
    (try
      (block? b)
      (let [file-name (:file-name b)
            ch (get-or-open-channel openChannels dbPath file-name)
            offset (* (:blockn b) blockSize)
            buffer (.rewind p)]
        (locking ch
          (.position ^FileChannel ch ^long offset)
          (.read ^FileChannel ch ^ByteBuffer buffer)))

      (catch Exception e
        (throw (ex-info "Failed to read block"
                        {:block b
                         :file-name (:file-name b)
                         :blockn (:blockn b)
                         :offset (* (:blockn b) blockSize)}
                        e)))))

  (writeTo [_ b p]
    (try
      (block? b)
      (let [file-name (:file-name b)
            ch (get-or-open-channel openChannels dbPath file-name)
            offset (* (:blockn b) blockSize)
            buffer (.rewind p)]
        (locking ch
          (.position ^FileChannel ch ^long offset)
          (.write ^FileChannel ch ^ByteBuffer buffer)))

      (catch Exception e
        (throw (ex-info "Failed to write block"
                        {:block b
                         :file-name (:file-name b)
                         :blockn (:blockn b)
                         :offset (* (:blockn b) blockSize)}
                        e)))))

  (appendNew [_ fileName]
    (try
      (let [ch (get-or-open-channel openChannels dbPath fileName)
            new-block-num (quot (.size ^FileChannel ch) blockSize)
            offset (* new-block-num blockSize)
            buffer (ByteBuffer/allocate blockSize)]
        (locking ch
          (.position ^FileChannel ch ^long offset)
          (.write ^FileChannel ch buffer))
        {:file-name fileName :blockn new-block-num})

      (catch Exception e
        (throw (ex-info "Failed to append block"
                        {:file-name fileName}
                        e))))))

(defn make-file-mngr
  ([db-path block-size]
   (make-file-mngr db-path block-size 100))
  ([db-path block-size max-open-files]
   (let [file (File. db-path)
         is-new? (not (.exists file))]
     (when is-new?
       (.mkdir file))
     (doseq [temp-file (eduction
                        (filter #(.startsWith % "temp"))
                        (.list file))]
       (try
         (-> (str db-path "/" temp-file)
             (File.)
             (.delete))
         (catch Exception e
           (println "Warning: Failed to delete temp file" temp-file ":" (.getMessage e)))))
     (->FileMngr (lru/make-lru-cache max-open-files #(.close ^FileChannel %2))
                 db-path
                 block-size))))
