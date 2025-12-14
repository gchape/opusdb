(ns opusdb.log
  (:refer-clojure :exclude [+ - inc flush])
  (:require [opusdb.page :as p]
            [opusdb.file :as fm])
  (:import [opusdb.page Page]
           [opusdb.file FileMgr]))

(def +
  (fn* [x y & more]
       (reduce unchecked-add-int 0 (into [x y] more))))

(def -
  (fn* [x y] (unchecked-subtract-int x y)))

(def inc
  (fn* [x] (unchecked-inc-int x)))

(deftype LogMgr [^FileMgr file-mgr
                 ^String file-name
                 ^Page page
                 ^clojure.lang.ITransientMap state]

  clojure.lang.Seqable
  (^clojure.lang.ISeq seq [_]
    (let [block-size (fm/block-size file-mgr)
          ^Page page (p/make-page (byte-array block-size))]
      (loop [% (state :block-id)
             position nil
             result '()]
        (let [position (or position
                           (do
                             (fm/read file-mgr % page)
                             (.getInt page 0)))
              index (:index %)]
          (cond
            (and (zero? index) (>= position block-size))
            result

            (>= position block-size)
            (recur {:file-name file-name :index (dec index)} nil result)

            :else
            (let [rec (.getBytes page position)
                  next-pos (+ 4 position (alength rec))]
              (recur % next-pos (cons rec result)))))))))
(defn flush
  ([^LogMgr log-mgr]
   (fm/write (.-file-mgr log-mgr) ((.-state log-mgr) :block-id) (.-page log-mgr))
   (assoc! (.-state log-mgr) :last-saved-lsn ((.-state log-mgr) :latest-lsn)))
  ([^LogMgr log-mgr lsn]
   (when (>= lsn ((.-state log-mgr) :last-saved-lsn))
     (flush log-mgr))))

(defn append [^LogMgr log-mgr ^bytes rec]
  (locking log-mgr
    (let [boundary (.getInt ^Page (.-page log-mgr) 0)
          length (alength rec)
          bytes-needed (+ length 4)]
      (when (< (- boundary bytes-needed) 4)
        (flush log-mgr)
        (let [new-block-id (fm/append (.-file-mgr log-mgr) (.-file-name log-mgr))
              free-boundary (fm/block-size (.-file-mgr log-mgr))]
          (.setInt ^Page (.-page log-mgr) 0 free-boundary)
          (assoc! (.-state log-mgr) :block-id new-block-id)
          (fm/write (.-file-mgr log-mgr) new-block-id (.-page log-mgr))))
      (let [boundary (.getInt ^Page (.-page log-mgr) 0)
            position (- boundary bytes-needed)
            state (.-state log-mgr)]
        (.setBytes ^Page (.-page log-mgr) position rec)
        (.setInt ^Page (.-page log-mgr) 0 position)
        (assoc! state :latest-lsn (inc (state :latest-lsn)))
        (state :latest-lsn)))))

(defn make-log-mgr
  [^FileMgr file-mgr ^String file-name]
  (let [^Page page (p/make-page (long (fm/block-size file-mgr)))
        file-size (fm/file-size file-mgr file-name)
        block-id (if (zero? file-size)
                   (let [block-id (fm/append file-mgr file-name)
                         boundary (fm/block-size file-mgr)]
                     (.setInt page 0 boundary)
                     (fm/write file-mgr block-id page)
                     block-id)
                   (let [block-id {:file-name file-name
                                   :index (dec (quot file-size (fm/block-size file-mgr)))}]
                     (fm/read file-mgr block-id page)
                     block-id))
        state (transient {:block-id block-id
                          :last-saved-lsn 0
                          :latest-lsn 0})]
    (->LogMgr file-mgr file-name page state)))
