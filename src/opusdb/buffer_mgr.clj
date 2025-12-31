(ns opusdb.buffer-mgr
  (:require [opusdb.buffer :as b])
  (:import [opusdb.buffer Buffer]
           [opusdb.file FileMgr]
           [opusdb.log LogMgr]))

(defrecord BufferMgr
           [block-table
            unpinned
            state
            timeout])

(defn- find-buffer [^BufferMgr bm block-id]
  (get @(:block-table bm) block-id))

(defn- evict-buffer [^BufferMgr bm]
  (let [buf (first @(:unpinned bm))]
    (when buf
      (swap! (:unpinned bm) disj buf)
      buf)))

(defn available [^BufferMgr bm]
  (:available @(:state bm)))

(defn flush-all [^BufferMgr bm tx-id]
  (doseq [^Buffer buf (vals @(:block-table bm))]
    (when (= tx-id (:tx-id (.-state buf)))
      (b/flush buf))))

(defn unpin-buffer [^BufferMgr bm ^Buffer buf]
  (b/unpin buf)
  (when (b/unpinned? buf)
    (swap! (:unpinned bm) conj buf)
    (swap! (:state bm) update :available inc)))

(defn pin-buffer [^BufferMgr bm block-id]
  (let [start (System/currentTimeMillis)]
    (loop []
      (cond
        (find-buffer bm block-id)
        (let [buf (find-buffer bm block-id)]
          (when (b/unpinned? buf)
            (swap! (:unpinned bm) disj buf)
            (swap! (:state bm) update :available dec))
          (b/pin buf)
          buf)

        :else
        (if-let [^Buffer victim (evict-buffer bm)]
          (let [old-block (:block-id (.-state victim))]
            (when old-block (swap! (:block-table bm) dissoc old-block))
            (b/assign-to-block victim block-id)
            (swap! (:block-table bm) assoc block-id victim)
            (b/pin victim)
            (swap! (:state bm) update :available dec)
            victim)

          (if (> (- (System/currentTimeMillis) start) (:timeout bm))
            (throw (ex-info "Buffer abort: waiting too long" {:block-id block-id}))
            (do
              (Thread/sleep ^int (:timeout bm))
              (recur))))))))

(defn make-buffer-mgr [^FileMgr file-mgr ^LogMgr log-mgr pool-size]
  (let [buffers   (repeatedly pool-size #(b/make-buffer file-mgr log-mgr))
        table     (atom {})
        unpinned  (atom (set buffers))
        state     (atom {:available pool-size})]
    (->BufferMgr table unpinned state 10000)))
