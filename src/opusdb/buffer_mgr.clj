(ns opusdb.buffer-mgr
  (:require [opusdb.buffer :as buffer]
            [opusdb.lru :as lru])
  (:import [opusdb.buffer Buffer]
           [opusdb.file FileMgr]
           [opusdb.log LogMgr]
           [java.util LinkedHashMap]))

(defn- find-existing-buffer
  [^LinkedHashMap buffers blk]
  (let [values (iterator-seq (.iterator (.values buffers)))]
    (first (filter #(let [b (buffer/block %)]
                      (and (some? b) (= b blk)))
                   values))))

(defn- choose-unpinned-buffer
  [^LinkedHashMap buffers]
  (loop [it (.iterator (.values buffers))]
    (when (.hasNext it)
      (let [next (.next it)]
        (if (buffer/pinned? next)
          (recur it)
          next)))))

(defrecord BufferMgr [^LinkedHashMap buffers
                      ^clojure.lang.ITransientMap state
                      ^long timeout]

  Object
  (toString [_]
    (str "BufferMgr[available=" (:available state)
         ", timeout=" timeout "ms]")))

(defn available [^BufferMgr mgr]
  (locking mgr
    (:available (.-state mgr))))

(defn flush-all [^BufferMgr mgr tx-id]
  (locking mgr
    (let [values (iterator-seq (.iterator (.values (:buffers mgr))))]
      (run! #(when (= (buffer/txid %) tx-id)
               (buffer/flush %))
            values))))

(defn unpin-buffer [^BufferMgr mgr buf]
  (locking mgr
    (buffer/unpin buf)
    (when-not (buffer/pinned? buf)
      (let [state (.-state mgr)
            available (:available state)]
        (conj! state
               {:available (unchecked-inc-int available)}))
      (.notifyAll mgr))))

(defn pin-buffer [^BufferMgr mgr blk]
  (locking mgr
    (try
      (let [buffers (:buffers mgr)
            start-time (System/currentTimeMillis)
            timeout (:timeout mgr)]
        (loop []
          (let [^Buffer buf (or (find-existing-buffer buffers blk)
                                (when-let [^Buffer unpinned (choose-unpinned-buffer buffers)]
                                  (buffer/assign-to-block unpinned blk)
                                  unpinned))]
            (if (nil? buf)
              (if (> (- (System/currentTimeMillis) start-time) timeout)
                (throw (ex-info "Buffer abort: waiting too long" {:block blk}))
                (do (.wait mgr timeout)
                    (recur)))
              (do
                (when-not (buffer/pinned? buf)
                  (let [state (.-state mgr)
                        available (:available state)]
                    (conj! state
                           {:available (unchecked-dec-int available)})))
                (buffer/pin buf)
                (.get buffers buf)
                buf)))))
      (catch InterruptedException _
        (throw (ex-info "Buffer abort: interrupted" {:block blk}))))))

(defn make-buffer-mgr
  [^FileMgr file-mgr ^LogMgr log-mgr pool-size]
  (let [^LinkedHashMap lru-cache (lru/make-lru-cache pool-size nil)
        buffers (repeatedly pool-size #(buffer/make-buffer file-mgr log-mgr))
        state (transient {:available pool-size})]
    (doseq [buf buffers]
      (.put lru-cache buf buf))
    (->BufferMgr lru-cache state 10000)))
