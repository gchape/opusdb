(ns opusdb.buffer
  (:refer-clojure :exclude [flush])
  (:require [opusdb.page :as p]
            [opusdb.file]
            [opusdb.log])
  (:import [opusdb.file FileMgr]
           [opusdb.log LogMgr]
           [opusdb.page Page]))

(defrecord Buffer [^FileMgr file-mgr
                   ^Page pg
                   ^LogMgr log-mgr
                   ^clojure.lang.ITransientMap state]

  Object
  (toString [_]
    (let [s state]
      (str "Buffer[block=" (:block s)
           ", pinned=" (pos? (:pin-count s))
           ", txid=" (:txid s) "]"))))

(defn block [^Buffer buffer]
  (:block (.-state buffer)))

(defn txid [^Buffer buffer]
  (:txid (.-state buffer)))

(defn lsn [^Buffer buffer]
  (:lsn (.-state buffer)))

(defn pinned? [^Buffer buffer]
  (pos? (:pin-count (.-state buffer))))

(defn pin-count [^Buffer buffer]
  (:pin-count (.-state buffer)))

(defn mark-dirty [^Buffer buffer new-tx-id new-lsn]
  (when (neg? new-tx-id)
    (throw (IllegalArgumentException.
            "Transaction ID must be non-negative")))
  (let [state (.-state buffer)
        _ (assoc! state :txid new-tx-id)]
    (when (and new-lsn (not (neg? new-lsn)))
      (assoc! state :lsn new-lsn))))

(defn pin [^Buffer buffer]
  (let [state (.-state buffer)]
    (assoc! state :pin-count
            (unchecked-inc-int (:pin-count state)))))

(defn unpin [^Buffer buffer]
  (let [state (.-state buffer)
        cnt   (:pin-count state)]
    (when (zero? cnt)
      (throw (IllegalStateException.
              "Cannot unpin buffer with pin-count 0")))
    (assoc! state :pin-count (unchecked-dec-int cnt))))

(defn flush [^Buffer buffer]
  (let [state (.-state buffer)
        tx-id (:txid state)]
    (when (not= tx-id -1)
      (let [blk (:block state)]
        (when-not blk
          (throw (IllegalStateException.
                  "Cannot flush: buffer has no assigned block")))
        (.flush (.-log-mgr buffer) (:lsn state))
        (.write (.-file-mgr buffer) blk (.-pg buffer))
        (assoc! state :txid -1)))))

(defn assign-to-block [^Buffer buffer new-block]
  (flush buffer)
  (.read (.-file-mgr buffer) new-block (.-pg buffer))
  (conj! (.-state buffer)
         {:block new-block
          :pin-count 0
          :txid -1
          :lsn -1}))

(defn page [^Buffer buffer]
  (.-pg buffer))

(defn make-buffer
  [^FileMgr file-mgr ^LogMgr log-mgr]
  (let [block-size (.blockSize file-mgr)
        pg (p/make-page block-size)]
    (Buffer. file-mgr
             pg
             log-mgr
             (transient {:block nil
                         :pin-count 0
                         :txid -1
                         :lsn -1}))))
