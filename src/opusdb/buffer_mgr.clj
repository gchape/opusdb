(ns opusdb.buffer-mgr
  (:require [opusdb.buffer :as bm]
            [opusdb.lru :as l])
  (:import [opusdb.buffer Buffer]
           [opusdb.file FileMgr]
           [opusdb.log LogMgr]
           [java.util LinkedHashMap]))

(defn- find-existing-buffer
  [^LinkedHashMap buffer-pool block-id]
  (let [values (iterator-seq (.iterator (.values buffer-pool)))]
    (first (filter #(let [% (bm/block-id %)]
                      (and (some? %) (= % block-id)))
                   values))))

(defn- choose-unpinned-buffer
  [^LinkedHashMap buffer-pool]
  (loop [it (.iterator (.values buffer-pool))]
    (when (.hasNext it)
      (let [next (.next it)]
        (if (bm/pinned? next)
          (recur it)
          next)))))

(defrecord BufferMgr [^LinkedHashMap buffer-pool
                      ^clojure.lang.ITransientMap state
                      ^long timeout])

(defn available [^BufferMgr buffer-mgr]
  (locking buffer-mgr
    (:available (.-state buffer-mgr))))

(defn flush-all [^BufferMgr buffer-mgr txid]
  (locking buffer-mgr
    (let [values (iterator-seq (.iterator (.values ^LinkedHashMap (:buffer-pool buffer-mgr))))]
      (run! #(when (= (bm/txid %) txid)
               (bm/flush %))
            values))))

(defn unpin-buffer [^BufferMgr buffer-mgr buffer]
  (locking buffer-mgr
    (bm/unpin buffer)
    (when-not (bm/pinned? buffer)
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
                                     (bm/assign-to-block unpinned block-id)
                                     unpinned))]
            (if (nil? buffer)
              (if (> (- (System/currentTimeMillis) start-time) timeout)
                (throw (ex-info "Buffer abort: waiting too long" {:block-id block-id}))
                (do (.wait buffer-mgr timeout)
                    (recur)))
              (do
                (when-not (bm/pinned? buffer)
                  (let [state (.-state buffer-mgr)
                        available (:available state)]
                    (conj! state
                           {:available (unchecked-dec-int available)})))
                (bm/pin buffer)
                (.get buffer-pool buffer)
                buffer)))))
      (catch InterruptedException _
        (throw (ex-info "Buffer abort: interrupted" {:block-id block-id}))))))

(defn make-buffer-mgr
  [^FileMgr file-mgr ^LogMgr log-mgr buffer-pool-size]
  (let [^LinkedHashMap lru-cache (l/make-lru-cache buffer-pool-size nil)
        buffer-pool (repeatedly buffer-pool-size #(bm/make-buffer file-mgr log-mgr))
        state (transient {:available buffer-pool-size})]
    (doseq [buffer buffer-pool]
      (.put lru-cache buffer buffer))
    (->BufferMgr lru-cache state 10000)))
