(ns opusdb.log
  (:require [opusdb.file]
            [opusdb.page :as page]
            [opusdb.uncheked.math :refer [+int -int]]))

(definterface ILogMngr
  (^void flush [])
  (^void flush [^int lsn])
  (^int append [^bytes rec])
  (^clojure.lang.IPersistentVector toVector []))

(deftype LogMngr [^opusdb.file.FileMngr file-mngr
                  ^String file-name
                  ^opusdb.page.Page page
                  ^:unsynchronized-mutable ^clojure.lang.IPersistentMap block
                  ^:unsynchronized-mutable ^int last-saved-lsn
                  ^:unsynchronized-mutable ^int latest-lsn]
  ILogMngr
  (append [this rec]
    (locking this
      (let [boundary (.getInt page 0)
            rec-length (alength rec)
            bytes-needed (+int rec-length 4)]
        (when (< (-int boundary bytes-needed) 4)
          (.flush this)
          (let [new-block (.append file-mngr file-name)
                boundary (.blockSize file-mngr)]
            (.setInt page 0 boundary)
            (set! block new-block)
            (.writeTo file-mngr block page)))
        (let [boundary (.getInt page 0)
              write-pos (-int boundary bytes-needed)]
          (.setBytes page write-pos rec)
          (.setInt page 0 write-pos)
          (set! latest-lsn (unchecked-inc-int latest-lsn))
          latest-lsn))))

  (flush [_]
    (.writeTo file-mngr block page)
    (set! last-saved-lsn latest-lsn))

  (flush [this lsn]
    (when (>= lsn last-saved-lsn)
      (.flush this)))

  (toVector [_]
    (let [block-size (.blockSize file-mngr)
          ^opusdb.page.Page temp-page (page/make-page (byte-array block-size))]
      (loop [current-block block
             read-pos nil
             result []]
        (let [read-pos (or read-pos
                           (do (.readFrom file-mngr current-block temp-page)
                               (.getInt temp-page 0)))
              {:keys [blockn]} current-block]
          (cond
            (and (zero? blockn)
                 (>= read-pos block-size)) result

            (>= read-pos block-size) (recur {:file-name file-name :blockn (dec blockn)}
                                            nil
                                            result)

            :else
            (let [rec (.getBytes temp-page read-pos)
                  next-read-pos (+int 4 read-pos (alength rec))]
              (recur current-block
                     next-read-pos
                     (conj result rec)))))))))

(def make-log-mngr
  (fn* [^opusdb.file.FileMngr file-mngr ^String file-name]
       (let [^opusdb.page.Page page (page/make-page (long (.blockSize file-mngr)))
             length (.length file-mngr file-name)
             block (if (zero? length)
                     (let [block (.append file-mngr file-name)
                           boundary (.blockSize file-mngr)]
                       (.setInt page 0 boundary)
                       (.writeTo file-mngr block page)
                       block)
                     (let [block {:file-name file-name
                                  :blockn (dec (quot length (.blockSize file-mngr)))}]
                       (.readFrom file-mngr block page)
                       block))]
         (->LogMngr file-mngr
                    file-name
                    page
                    block
                    0
                    0))))
