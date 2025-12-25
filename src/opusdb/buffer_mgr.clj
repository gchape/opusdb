(ns opusdb.buffer-mgr
  (:require [opusdb.buffer :as b]
            [opusdb.lru :as l])
  (:import [opusdb.buffer Buffer]
           [opusdb.file FileMgr]
           [opusdb.log LogMgr]
           [java.util LinkedHashMap]))

(defn- find-existing-buffer
  [^LinkedHashMap buffer-pool block-id]
  (let [values (iterator-seq (.iterator (vals buffer-pool)))]
    (first (filter #(= (b/block-id %) block-id)
                   values))))

(defn- choose-unpinned-buffer
  [^LinkedHashMap buffer-pool]
  (let [values (iterator-seq (.iterator (vals buffer-pool)))]
    (first (filter #(not (b/pinned? %))
                   values))))

(defrecord BufferMgr [^LinkedHashMap buffer-pool
                      ^clojure.lang.ITransientMap state
                      ^long timeout])

(defn available [^BufferMgr buffer-mgr]
  (locking buffer-mgr
    (:available (.-state buffer-mgr))))

(defn flush-all [^BufferMgr buffer-mgr tx-id]
  (locking buffer-mgr
    (let [values (iterator-seq (.iterator (.values ^LinkedHashMap (:buffer-pool buffer-mgr))))]
      (run! #(when (= (b/tx-id %) tx-id)
               (b/flush %))
            values))))

(defn unpin-buffer [^BufferMgr buffer-mgr buffer]
  (locking buffer-mgr
    (b/unpin buffer)
    (when-not (b/pinned? buffer)
      (let [state (.-state buffer-mgr)
            available (:available state)]
        (conj! state
               {:available (unchecked-inc-int available)}))
      (.notifyAll buffer-mgr))))

(defn pin-buffer [^BufferMgr buffer-mgr block-id]
  (locking buffer-mgr
    (try
      (let [^LinkedHashMap buffer-pool (:buffer-pool buffer-mgr)
            start-time (System/currentTimeMillis)
            timeout (:timeout buffer-mgr)]
        (loop []
          (let [^Buffer buffer (or (find-existing-buffer buffer-pool block-id)
                                   (when-let [^Buffer unpinned (choose-unpinned-buffer buffer-pool)]
                                     (b/assign-to-block unpinned block-id)
                                     unpinned))]
            (if (nil? buffer)
              (if (> (- (System/currentTimeMillis) start-time) timeout)
                (throw (ex-info "Buffer abort: waiting too long" {:block-id block-id}))
                (do (.wait buffer-mgr timeout)
                    (recur)))
              (do
                (when-not (b/pinned? buffer)
                  (let [state (.-state buffer-mgr)
                        available (:available state)]
                    (conj! state
                           {:available (unchecked-dec-int available)})))
                (b/pin buffer)
                (.get buffer-pool buffer)
                buffer)))))
      (catch InterruptedException _
        (throw (ex-info "Buffer abort: interrupted" {:block-id block-id}))))))

(defn make-buffer-mgr
  [^FileMgr file-mgr ^LogMgr log-mgr buffer-pool-size]
  (let [^LinkedHashMap lru-cache (l/make-lru-cache buffer-pool-size nil)
        buffer-pool (repeatedly buffer-pool-size #(b/make-buffer file-mgr log-mgr))
        state (transient {:available buffer-pool-size})]
    (doseq [buffer buffer-pool]
      (.put lru-cache buffer buffer))
    (->BufferMgr lru-cache state 10000)))
