(ns opusdb.io.file-mgr
  (:refer-clojure :exclude [read])
  (:require
   [opusdb.cache.lru :as lru])
  (:import
   [java.io File]
   [java.nio ByteBuffer]
   [java.nio.channels FileChannel]
   [java.nio.file StandardOpenOption]
   [java.util LinkedHashMap]))

(defn- eviction-fn
  [{:keys [_ value]}]
  (.close ^FileChannel value)
  true)

(defn- get-lock
  [file-mgr file-name]
  (nth (:locks file-mgr)
       (mod (hash file-name) (count (:locks file-mgr)))))

(defn- make-channel
  [^LinkedHashMap channel-pool db-dir file-name]
  (locking channel-pool
    (let [path (.toPath (File. (str db-dir "/" file-name)))
          channel (FileChannel/open
                   path
                   (into-array StandardOpenOption
                               [StandardOpenOption/READ
                                StandardOpenOption/WRITE
                                StandardOpenOption/DSYNC
                                StandardOpenOption/CREATE]))]
      (.put channel-pool file-name channel)
      channel)))

(defn- with-channel
  [file-mgr file-name index f]
  (let [db-dir (:db-dir file-mgr)
        lock (get-lock file-mgr file-name)
        offset (* index (:block-size file-mgr))
        ^LinkedHashMap channel-pool (:channel-pool file-mgr)]
    (locking lock
      (f (or (.get channel-pool file-name)
             (make-channel channel-pool
                           db-dir
                           file-name)) offset))))

(defn block-size
  [file-mgr]
  (:block-size file-mgr))

(defn file-size
  [file-mgr file-name]
  (let [db-dir (:db-dir file-mgr)
        pathname (str db-dir "/" file-name)]
    (.length (File. pathname))))

(defn read
  [file-mgr {:keys [file-name index] :as block-id} ^ByteBuffer page]
  (try
    (with-channel file-mgr file-name index
      (fn [^FileChannel channel offset]
        (let [^ByteBuffer buffer (.rewind page)]
          (.position channel ^long offset)
          (.read channel buffer))))
    (catch Exception e
      (throw (ex-info "Failed to read block"
                      {:block-id block-id
                       :offset (* index (:block-size file-mgr))}
                      e)))))

(defn write!
  [file-mgr {:keys [file-name index] :as block-id} ^ByteBuffer page]
  (try
    (with-channel file-mgr file-name index
      (fn [^FileChannel channel offset]
        (let [^ByteBuffer buffer (.rewind page)]
          (.position channel ^long offset)
          (.write channel buffer))))
    (catch Exception e
      (throw (ex-info "Failed to write block"
                      {:block-id block-id
                       :offset (* index (:block-size file-mgr))}
                      e)))))

(defn append!
  [file-mgr file-name]
  (try
    (with-channel file-mgr file-name 0
      (fn [^FileChannel channel _]
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

(defrecord FileMgr [^String db-dir
                    ^int block-size
                    ^LinkedHashMap channel-pool
                    locks])

(defn make-file-mgr
  ([db-dir block-size]
   (make-file-mgr db-dir block-size 1000))
  ([db-dir block-size max-open-channels]
   (let [dir (File. ^String db-dir)
         new? (not (.exists dir))]
     (when new? (.mkdirs dir))
     (doseq [temp (eduction
                   (filter #(.startsWith ^String % "temp"))
                   (.list dir))]
       (try
         (.delete (File. (str db-dir "/" temp)))
         (catch Exception _
           (println "Warning: Failed to delete temp file" temp))))
     (->FileMgr db-dir
                block-size
                (lru/make-lru-cache max-open-channels eviction-fn)
                (vec (repeatedly 16 #(Object.)))))))