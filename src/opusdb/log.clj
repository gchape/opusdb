(ns opusdb.log
  (:refer-clojure :exclude [+ - inc])
  (:require [opusdb.page :as page])
  (:import [opusdb.page Page]
           [opusdb.file FileMgr]))

(def +
  (fn* [x y & more]
       (reduce unchecked-add-int 0 (into [x y] more))))

(def -
  (fn* [x y] (unchecked-subtract-int x y)))

(def inc
  (fn* [x] (unchecked-inc-int x)))

(definterface ILogMgr
  (^void flush [])
  (^void flush [^int lsn])
  (^int append [^bytes record])
  (^clojure.lang.IPersistentVector getSnapshot []))

(deftype LogMgr [^FileMgr fileMgr
                 ^String fileName
                 ^Page page
                 ^:unsynchronized-mutable ^clojure.lang.IPersistentMap block
                 ^:unsynchronized-mutable ^int last-saved-lsn
                 ^:unsynchronized-mutable ^int latest-lsn]

  ILogMgr

  (append [this record]
    (locking this
      (let [boundary (.getInt page 0)
            length (alength record)
            bytes-needed (+ length 4)]
        (when (< (- boundary bytes-needed) 4)
          (.flush this)
          (let [new-block (.append fileMgr fileName)
                free-boundary (.blockSize fileMgr)]
            (.setInt page 0 free-boundary)
            (set! block new-block)
            (.write fileMgr block page)))
        (let [boundary (.getInt page 0)
              position (- boundary bytes-needed)]
          (.setBytes page position record)
          (.setInt page 0 position)
          (set! latest-lsn (int (inc latest-lsn)))
          latest-lsn))))

  (flush [_]
    (.write fileMgr block page)
    (set! last-saved-lsn latest-lsn))

  (flush [this lsn]
    (when (>= lsn last-saved-lsn)
      (.flush this)))

  (getSnapshot [_]
    (let [block-size (.blockSize fileMgr)
          ^Page tmp-page (page/make-page (byte-array block-size))]
      (loop [b block
             position nil
             result []]
        (let [position (or position
                           (do
                             (.read fileMgr b tmp-page)
                             (.getInt tmp-page 0)))
              block-id (:block-id b)]
          (cond
            ;; reached first block completely scanned
            (and (zero? block-id) (>= position block-size))
            result

            ;; move to previous block
            (>= position block-size)
            (recur {:file-name fileName :block-id (dec block-id)} nil result)

            :else
            (let [rec (.getBytes tmp-page position)
                  next-pos (+ 4 position (alength rec))]
              (recur b next-pos (conj result rec)))))))))

(defn make-log-mgr
  [^FileMgr file-mgr ^String file-name]
  (let [^Page page (page/make-page (long (.blockSize file-mgr)))
        file-size (.fileSize file-mgr file-name)
        block (if (zero? file-size)
                (let [block (.append file-mgr file-name)
                      boundary (.blockSize file-mgr)]
                  (.setInt page 0 boundary)
                  (.write file-mgr block page)
                  block)
                (let [block {:file-name file-name
                             :block-id (dec (quot file-size (.blockSize file-mgr)))}]
                  (.read file-mgr block page)
                  block))]
    (->LogMgr file-mgr file-name page block 0 0)))
