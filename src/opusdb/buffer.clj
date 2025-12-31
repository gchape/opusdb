(ns opusdb.buffer
  (:refer-clojure :exclude [flush])
  (:require [opusdb.log :as lm]
            [opusdb.file :as f]
            [opusdb.page :as p])
  (:import [opusdb.file FileMgr]
           [opusdb.log LogMgr]
           [io.netty.buffer ByteBuf]))

(defrecord Buffer [^FileMgr file-mgr
                   ^LogMgr log-mgr
                   ^ByteBuf page
                   ^clojure.lang.ITransientMap state])

(defn pinned? [^Buffer buf]
  (pos? (:pin-count (.-state buf))))

(defn unpinned? [^Buffer buf]
  (not (pinned? buf)))

(defn mark-dirty [^Buffer buf new-tx-id new-lsn]
  (when (neg? new-tx-id)
    (throw (IllegalArgumentException.
            "Transaction ID must be non-negative")))
  (let [state (.-state buf)
        _ (assoc! state :tx-id new-tx-id)]
    (when (and new-lsn (not (neg? new-lsn)))
      (assoc! state :lsn new-lsn))))

(defn pin [^Buffer buf]
  (let [state (.-state buf)]
    (assoc! state :pin-count (unchecked-inc-int (:pin-count state)))))

(defn unpin [^Buffer buf]
  (let [state (.-state buf)
        pin-count   (:pin-count state)]
    (when (zero? pin-count)
      (throw (IllegalStateException.
              "Cannot unpin buffer with pin-count 0")))
    (assoc! state :pin-count (unchecked-dec-int pin-count))))

(defn flush [^Buffer buf]
  (let [state (.-state buf)
        tx-id (:tx-id state)]
    (when (not= tx-id -1)
      (let [block-id (:block-id state)]
        (when-not block-id
          (throw (IllegalStateException.
                  "Cannot flush: buffer has no assigned block")))
        (lm/flush (.-log-mgr buf) (:lsn state))
        (f/write (.-file-mgr buf) block-id (.-page buf))
        (assoc! state :tx-id -1)))))

(defn assign-to-block [^Buffer buf block-id]
  (flush buf)
  (f/read (.-file-mgr buf) block-id (.-page buf))
  (conj! (.-state buf)
         {:block-id block-id
          :pin-count 0
          :tx-id -1
          :lsn -1}))

(defn make-buffer
  [^FileMgr file-mgr ^LogMgr log-mgr]
  (let [block-size (f/block-size file-mgr)
        page (p/make-page block-size)]
    (Buffer. file-mgr
             log-mgr
             page
             (transient {:block-id nil
                         :pin-count 0
                         :tx-id -1
                         :lsn -1}))))
