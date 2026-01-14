(ns opusdb.memory.buffer-mgr
  (:refer-clojure :exclude [vals key flush])
  (:require
   [opusdb.cache.splay :as cache]
   [opusdb.memory.buffer :as buff])
  (:import
   [opusdb.cache.splay Cache]
   [opusdb.io.file_mgr FileMgr]
   [opusdb.logging.log_mgr LogMgr]
   [opusdb.memory.buffer Buffer]))

(defn- vals
  [^Cache buffer-pool]
  (cache/vals buffer-pool))

(defn- key
  [block-id]
  (when block-id
    [(:file-name block-id) (:index block-id)]))

(defn- find-existing-buffer
  [^Cache buffer-pool block-id]
  (let [key (key block-id)]
    (cache/get buffer-pool key)))

(defn- find-unpinned-buffer
  [^Cache buffer-pool]
  (some #(when (buff/unpinned? %) %)
        (vals buffer-pool)))

(defrecord BufferMgr [^Cache buffer-pool
                      ^FileMgr file-mgr
                      ^LogMgr log-mgr
                      ^clojure.lang.Atom state
                      ^long timeout])

(defn available [^BufferMgr buffer-mgr]
  (locking buffer-mgr
    (:available @(:state buffer-mgr))))

(defn flush! [^BufferMgr buffer-mgr tx-id]
  (locking buffer-mgr
    (run! #(when (= (:tx-id @(:state %)) tx-id)
             (buff/flush! %))
          (vals (:buffer-pool buffer-mgr)))))

(defn unpin-buffer! [^BufferMgr buffer-mgr buffer]
  (locking buffer-mgr
    (buff/unpin! buffer)
    (when-not (buff/pinned? buffer)
      (swap! (:state buffer-mgr) update :available inc)
      (.notifyAll buffer-mgr))))

(defn pin-buffer! [^BufferMgr buffer-mgr block-id]
  (locking buffer-mgr
    (let [^Cache pool (:buffer-pool buffer-mgr)
          key (key block-id)
          state (:state buffer-mgr)
          timeout (:timeout buffer-mgr)
          start-time (System/currentTimeMillis)]
      (try
        (loop []
          (if-let [^Buffer buff
                   (or (find-existing-buffer pool block-id)

                       (when-let [^Buffer free (find-unpinned-buffer pool)]
                         (buff/assign-to-block! free block-id)
                         (cache/put! pool key free)
                         free)

                       (when (< @(:size pool) (:max-size pool))
                         (let [new-buff (buff/make-buffer (:file-mgr buffer-mgr)
                                                          (:log-mgr buffer-mgr))]
                           (buff/assign-to-block! new-buff block-id)
                           (cache/put! pool key new-buff)
                           new-buff)))]
            (do
              (when-not (buff/pinned? buff)
                (swap! state update :available dec))
              (buff/pin! buff)
              buff)
            (if (> (- (System/currentTimeMillis) start-time) timeout)
              (throw (ex-info "Buffer abort: waiting too long"
                              {:block-id block-id}))
              (do
                (.wait buffer-mgr timeout)
                (recur)))))
        (catch InterruptedException _
          (throw (ex-info "Buffer abort: interrupted"
                          {:block-id block-id})))))))

(defn make-buffer-mgr
  [^FileMgr file-mgr ^LogMgr log-mgr buffer-pool-size]
  (let [cache (cache/make-cache buffer-pool-size nil "LRU")
        state (atom {:available buffer-pool-size})]
    (->BufferMgr cache file-mgr log-mgr state 10000)))