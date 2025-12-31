(ns opusdb.log
  (:refer-clojure :exclude [flush])
  (:require [opusdb.page :as p]
            [opusdb.file :as fm])
  (:import [opusdb.file FileMgr]
           [io.netty.buffer ByteBuf]))

(deftype LogMgr [^FileMgr file-mgr
                 ^String file-name
                 ^ByteBuf page
                 ^clojure.lang.ITransientMap state]
  clojure.lang.Seqable
  (^clojure.lang.ISeq seq [_]
    (let [block-size (fm/block-size file-mgr)
          ^ByteBuf page (p/make-page block-size)
          {:keys [file-name index]} (:block-id state)]
      (reduce
       (fn [result idx]
         (let [block-id {:file-name file-name :index idx}]
           (fm/read file-mgr block-id page)
           (let [pos (long (.getInt page 0))]
             (loop [pos pos
                    acc result]
               (if (>= pos block-size)
                 acc
                 (let [^bytes rec (p/get-bytes page pos)]
                   (recur (+ pos 4 (alength rec))
                          (cons rec acc))))))))
       '()
       (range index -1 -1)))))

(defn flush
  ([^LogMgr log-mgr]
   (let [fm   (.-file-mgr log-mgr)
         page (.-page log-mgr)
         state   (.-state log-mgr)]
     (fm/write fm (state :block-id) page)
     (assoc! state :last-saved-lsn (state :latest-lsn))))
  ([^LogMgr log-mgr lsn]
   (when (>= lsn ((.-state log-mgr) :last-saved-lsn))
     (flush log-mgr))))

(defn append
  [^LogMgr log-mgr ^bytes rec]
  (locking log-mgr
    (let [^ByteBuf page (.-page log-mgr)
          fm       (.-file-mgr log-mgr)
          state    (.-state log-mgr)
          pos (.getInt page 0)
          length   (alength rec)
          bytes-needed (+ length 4)]
      (when (< pos (+ bytes-needed 4))
        (flush log-mgr)
        (let [new-block-id (fm/append fm (.-file-name log-mgr))]
          (.setInt page 0 (fm/block-size fm))
          (assoc! state :block-id new-block-id)))
      (let [pos (.getInt page 0)
            next-pos (- pos bytes-needed)
            next-lsn (inc (state :latest-lsn))
            _ (assoc! state :latest-lsn next-lsn)]
        (p/set-bytes page next-pos rec)
        (.setInt page 0 next-pos)
        next-lsn))))

(defn make-log-mgr
  [^FileMgr file-mgr ^String file-name]
  (let [block-size (fm/block-size file-mgr)
        ^ByteBuf page (p/make-page block-size)
        file-size (fm/file-size file-mgr file-name)
        block-id
        (if (zero? file-size)
          (let [block-id (fm/append file-mgr file-name)]
            (.setInt page 0 block-size)
            (fm/write file-mgr block-id page)
            block-id)
          (let [block-id {:file-name file-name
                          :index (dec (quot file-size block-size))}]
            (fm/read file-mgr block-id page)
            block-id))
        state (transient {:block-id block-id
                          :last-saved-lsn 0
                          :latest-lsn 0})]
    (->LogMgr file-mgr file-name page state)))
