(ns opusdb.log-mgr
  (:refer-clojure :exclude [flush])
  (:require
   [opusdb.file-mgr :as fm]
   [opusdb.page :as p])
  (:import
   [java.nio ByteBuffer]
   [opusdb.file_mgr FileMgr]))

(deftype LogMgr [^FileMgr file-mgr
                 ^String file-name
                 ^ByteBuffer page
                 ^clojure.lang.Atom state]

  clojure.lang.Seqable
  (^clojure.lang.ISeq seq [_]
    (let [block-size (fm/block-size file-mgr)
          ^ByteBuffer page (p/make-page block-size)
          {:keys [file-name index]} (:block-id @state)]
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

(defn flush!
  ([^LogMgr log-mgr]
   (let [fm   (.-file-mgr log-mgr)
         page (.-page log-mgr)
         state (.-state log-mgr)]
     (fm/write! fm (:block-id @state) page)
     (swap! state assoc :last-saved-lsn (:latest-lsn @state))))
  ([^LogMgr log-mgr lsn]
   (when (>= lsn (:last-saved-lsn @(.-state log-mgr)))
     (flush! log-mgr))))

(defn append!
  [^LogMgr log-mgr ^bytes rec]
  (locking log-mgr
    (let [^ByteBuffer page (.-page log-mgr)
          fm       (.-file-mgr log-mgr)
          state-atom (.-state log-mgr)
          pos (.getInt page 0)
          length   (alength rec)
          bytes-needed (+ length 4)]
      (when (< pos (+ bytes-needed 4))
        (flush! log-mgr)
        (let [new-block-id (fm/append! fm (.-file-name log-mgr))]
          (.putInt page 0 (fm/block-size fm))
          (swap! state-atom assoc :block-id new-block-id)))
      (let [pos (.getInt page 0)
            next-pos (- pos bytes-needed)
            next-lsn (inc (:latest-lsn @state-atom))]
        (swap! state-atom assoc :latest-lsn next-lsn)
        (p/put-bytes page next-pos rec)
        (.putInt page 0 next-pos)
        next-lsn))))

(defn make-log-mgr
  [^FileMgr file-mgr ^String file-name]
  (let [block-size (fm/block-size file-mgr)
        ^ByteBuffer page (p/make-page block-size)
        file-size (fm/file-size file-mgr file-name)
        block-id
        (if (zero? file-size)
          (let [block-id (fm/append! file-mgr file-name)]
            (.putInt page 0 block-size)
            (fm/write! file-mgr block-id page)
            block-id)
          (let [block-id {:file-name file-name
                          :index (dec (quot file-size block-size))}]
            (fm/read file-mgr block-id page)
            block-id))
        state (atom {:block-id block-id
                     :last-saved-lsn 0
                     :latest-lsn 0})]
    (->LogMgr file-mgr file-name page state)))