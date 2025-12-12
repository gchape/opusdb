(ns opusdb.file
  (:require [opusdb.page]
            [opusdb.lru :as lru]
            [opusdb.uncheked.math :refer [*int]])
  (:import [java.io File]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file StandardOpenOption]
           [java.util LinkedHashMap]))

(definterface IFileMngr
  (^int blockSize [])
  (^long length [^String fileName])
  (^void readFrom [^clojure.lang.IPersistentMap block ^opusdb.page.Page page])
  (^void writeTo [^clojure.lang.IPersistentMap block ^opusdb.page.Page page])
  (^clojure.lang.IPersistentMap append [^String fileName]))

(def ^:private get-or-open-channel
  (fn* [lru-cache db-path file-name]
       (locking lru-cache
         (or (.get ^LinkedHashMap lru-cache file-name)
             (let [path (.toPath (File. (str db-path "/" file-name)))
                   ch (FileChannel/open path (into-array StandardOpenOption
                                                         [StandardOpenOption/READ
                                                          StandardOpenOption/WRITE
                                                          StandardOpenOption/DSYNC
                                                          StandardOpenOption/CREATE]))]
               (.put ^LinkedHashMap lru-cache file-name ch)
               ch)))))

(defrecord FileMngr [^LinkedHashMap open-channels
                     ^String db-path
                     ^int block-size
                     strip-locks]
  IFileMngr
  (blockSize [_]
    block-size)

  (length [_ file-name]
    (-> (str db-path "/" file-name)
        (File.)
        (.length)))

  (readFrom [_ {:keys [file-name blockn] :as block} page]
    (try
      (let [ch (get-or-open-channel open-channels db-path file-name)
            offset (*int blockn block-size)
            buffer (.rewind page)
            lock (strip-locks (mod (hash file-name) (count strip-locks)))]
        (locking lock
          (.position ^FileChannel ch ^long offset)
          (.read ^FileChannel ch ^ByteBuffer buffer)))
      (catch Exception e
        (throw (ex-info "Failed to read block"
                        {:block block
                         :file-name file-name
                         :blockn blockn
                         :offset (*int blockn block-size)}
                        e)))))

  (writeTo [_ {:keys [file-name blockn] :as block} page]
    (try
      (let [ch (get-or-open-channel open-channels db-path file-name)
            offset (*int blockn block-size)
            buffer (.rewind page)
            lock (strip-locks (mod (hash file-name) (count strip-locks)))]
        (locking lock
          (.position ^FileChannel ch ^long offset)
          (.write ^FileChannel ch ^ByteBuffer buffer)))
      (catch Exception e
        (throw (ex-info "Failed to write block"
                        {:block block
                         :file-name file-name
                         :blockn blockn
                         :offset (*int blockn block-size)}
                        e)))))

  (append [_ file-name]
    (try
      (let [ch (get-or-open-channel open-channels db-path file-name)
            lock (strip-locks (mod (hash file-name) (count strip-locks)))]
        (locking lock
          (let [new-blockn (quot (.size ^FileChannel ch) block-size)
                offset (*int new-blockn block-size)
                buffer (ByteBuffer/allocate block-size)]
            (.position ^FileChannel ch ^long offset)
            (.write ^FileChannel ch buffer)
            {:file-name file-name :blockn new-blockn})))
      (catch Exception e
        (throw (ex-info "Failed to append block"
                        {:file-name file-name}
                        e))))))

(def ^:private get-or-make-file-mngr
  (memoize
   (fn* [db-path block-size max-open-files]
        (let [file (File. ^String db-path)
              is-new? (not (.exists file))]
          (when is-new?
            (.mkdirs file))
          (doseq [temp-file (eduction
                             (filter #(.startsWith ^String % "temp"))
                             (.list file))]
            (try
              (-> (str db-path "/" temp-file)
                  (File.)
                  (.delete))
              (catch Exception e
                (println "Warning: Failed to delete temp file" temp-file ":" (.getMessage e)))))
          (->FileMngr (lru/make-lru-cache max-open-files #(.close ^FileChannel %2))
                      db-path
                      block-size
                      (vec (repeatedly 16 #(Object.))))))))

(defn make-file-mngr
  ([db-path block-size]
   (make-file-mngr db-path block-size 100))
  ([db-path block-size max-open-files]
   (get-or-make-file-mngr db-path block-size max-open-files)))
