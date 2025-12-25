(ns opusdb.buffer
  (:refer-clojure :exclude [flush])
  (:require [opusdb.page :as p]
            [opusdb.log :as lm]
            [opusdb.file :as f])
  (:import [opusdb.file FileMgr]
           [opusdb.log LogMgr]
           [opusdb.page Page]))

(defrecord Buffer [^FileMgr file-mgr
                   ^LogMgr log-mgr
                   ^Page page
                   ^clojure.lang.ITransientMap state])

(defn block-id [^Buffer buffer]
  (:block-id (.-state buffer)))

(defn tx-id [^Buffer buffer]
  (:tx-id (.-state buffer)))

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
        _ (assoc! state :tx-id new-tx-id)]
    (when (and new-lsn (not (neg? new-lsn)))
      (assoc! state :lsn new-lsn))))

(defn pin [^Buffer buffer]
  (let [state (.-state buffer)]
    (assoc! state :pin-count
            (unchecked-inc-int (:pin-count state)))))

(defn unpin [^Buffer buffer]
  (let [state (.-state buffer)
        pin-count   (:pin-count state)]
    (when (zero? pin-count)
      (throw (IllegalStateException.
              "Cannot unpin buffer with pin-count 0")))
    (assoc! state :pin-count (unchecked-dec-int pin-count))))

(defn flush [^Buffer buffer]
  (let [state (.-state buffer)
        tx-id (:tx-id state)]
    (when (not= tx-id -1)
      (let [block-id (:block-id state)]
        (when-not block-id
          (throw (IllegalStateException.
                  "Cannot flush: buffer has no assigned block")))
        (lm/flush (.-log-mgr buffer) (:lsn state))
        (f/write (.-file-mgr buffer) block-id (.-page buffer))
        (assoc! state :tx-id -1)))))

(defn assign-to-block [^Buffer buffer block-id]
  (flush buffer)
  (f/read (.-file-mgr buffer) block-id (.-page buffer))
  (conj! (.-state buffer)
         {:block-id block-id
          :pin-count 0
          :tx-id -1
          :lsn -1}))

(defn page [^Buffer buffer]
  (.-page buffer))

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
