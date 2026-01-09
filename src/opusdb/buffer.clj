(ns opusdb.buffer
  (:refer-clojure :exclude [flush])
  (:require
   [opusdb.file-mgr :as fm]
   [opusdb.log-mgr :as lm]
   [opusdb.page :as p])
  (:import
   [java.nio ByteBuffer]
   [opusdb.file_mgr FileMgr]
   [opusdb.log_mgr LogMgr]))

(defrecord Buffer [^FileMgr file-mgr
                   ^LogMgr log-mgr
                   ^ByteBuffer page
                   ^clojure.lang.Atom state])

(defn pinned? [^Buffer buff]
  (pos? (:pin-count @(:state buff))))

(defn unpinned? [^Buffer buff]
  (not (pinned? buff)))

(defn smear! [^Buffer buff new-tx-id new-lsn]
  (when (neg? new-tx-id)
    (throw (IllegalArgumentException.
            "Transaction ID must be non-negative")))
  (swap! (:state buff)
         (fn [state]
           (let [state' (assoc state :tx-id new-tx-id)]
             (if (and new-lsn (not (neg? new-lsn)))
               (assoc state' :lsn new-lsn)
               state')))))

(defn pin! [^Buffer buff]
  (swap! (:state buff) update :pin-count inc))

(defn unpin! [^Buffer buff]
  (let [pin-count (:pin-count @(:state buff))]
    (when (zero? pin-count)
      (throw (IllegalStateException.
              "Cannot unpin buffer with pin-count 0")))
    (swap! (:state buff) update :pin-count dec)))

(defn flush! [^Buffer buff]
  (let [state @(:state buff)
        tx-id (:tx-id state)]
    (when (not= tx-id -1)
      (let [block-id (:block-id state)]
        (when-not block-id
          (throw (IllegalStateException.
                  "Cannot flush: buffer has no assigned block")))
        (lm/flush! (:log-mgr buff) (:lsn state))
        (fm/write! (:file-mgr buff) block-id (:page buff))
        (swap! (:state buff) assoc :tx-id -1)))))

(defn assign-to-block! [^Buffer buff block-id]
  (flush! buff)
  (fm/read (:file-mgr buff) block-id (:page buff))
  (reset! (:state buff)
          {:block-id block-id
           :pin-count 0
           :tx-id -1
           :lsn -1}))

(defn make-buffer
  [^FileMgr file-mgr ^LogMgr log-mgr]
  (let [block-size (fm/block-size file-mgr)
        page (p/make-page block-size)]
    (Buffer. file-mgr
             log-mgr
             page
             (atom {:block-id nil
                    :pin-count 0
                    :tx-id -1
                    :lsn -1}))))