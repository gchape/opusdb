(ns opusdb.buffer
  (:require [opusdb.page :as page]
            [opusdb.file :as file]
            [opusdb.log :as log]
            [opusdb.lru :as lru])
  (:import [opusdb.file FileMgr]
           [opusdb.log LogMgr]
           [opusdb.page Page]
           [java.util LinkedHashMap]))

(definterface IBuffer
  (^int txId [])
  (^opusdb.page.Page page [])
  (^clojure.lang.IPersistentMap block [])

  (^void markDirty [^int txn-id ^int lsn])
  (^void assignToBlock [^clojure.lang.IPersistentMap block])

  (^void pin [])
  (^void unpin [])
  (^boolean isPinned [])

  (^void flush []))

(deftype Buffer [^FileMgr file-mgr
                 ^Page page
                 ^LogMgr log-mgr
                 ^:unsynchronized-mutable ^clojure.lang.IPersistentMap block
                 ^:unsynchronized-mutable ^int pin-count
                 ^:unsynchronized-mutable ^int tx-id
                 ^:unsynchronized-mutable ^int lsn]

  IBuffer

  (page [_] page)

  (block [_] block)

  (txId [_] tx-id)

  (isPinned [_]
    (pos? pin-count))

  (markDirty [_ new-tx-id new-lsn]
    (when (neg? new-tx-id)
      (throw (IllegalArgumentException.
              "Transaction ID must be non-negative")))
    (set! tx-id new-tx-id)
    (when-not (neg? new-lsn)
      (set! lsn new-lsn)))

  (assignToBlock [this new-block]
    (.flush this)
    (set! block new-block)
    (.read file-mgr new-block page)
    (set! pin-count (int 0)))

  (flush [_]
    (when (not= tx-id -1)
      (when (nil? block)
        (throw (IllegalStateException.
                "Cannot flush: buffer has no assigned block")))
      (.flush log-mgr lsn)
      (.write file-mgr block page)
      (set! tx-id (int -1))))

  (pin [_]
    (set! pin-count (unchecked-inc-int pin-count)))

  (unpin [_]
    (set! pin-count (unchecked-dec-int pin-count))))

(defn make-buffer
  [^FileMgr file-mgr ^LogMgr log-mgr]
  (let [blockSize (.blockSize file-mgr)
        page (page/make-page blockSize)]
    (->Buffer
     file-mgr
     page
     log-mgr
     nil
     0
     -1
     -1)))

(definterface IBufferMgr
  (^int available [])

  (^void flushAll [^int txid])

  (^opusdb.buffer.Buffer pin [^clojure.lang.IPersistentMap block])
  (^void unpin [^opusdb.buffer.Buffer buffer]))

(defn- find-existing-buffer
  [^LinkedHashMap buffers block]
  (let [values (iterator-seq (.iterator (.values buffers)))]
    (first (filter #(let [b (.block ^Buffer %)]
                      (and (some? b) (= b block)))
                   values))))

(defn- choose-unpinned-buffer
  [^LinkedHashMap buffers]
  (loop [it (.iterator (.values buffers))]
    (when (.hasNext it)
      (let [next (.next it)]
        (if (.isPinned ^Buffer next)
          (recur it)
          next)))))

(deftype BufferMgr [^LinkedHashMap buffers
                    ^:unsynchronized-mutable ^int available
                    ^long timeout]
  :load-ns true
  IBufferMgr

  (available [this]
    (locking this
      available))

  (flushAll [this txid]
    (locking this
      (let [values (iterator-seq (.iterator (.values buffers)))]
        (run! #(when (= (.txId ^Buffer %) txid)
                 (.flush ^Buffer %))
              values))))

  (unpin [this buffer]
    (locking this
      (.unpin buffer)
      (when-not (.isPinned buffer)
        (set! available (unchecked-inc-int available))
        (.notifyAll this))))

  (pin [this block]
    (locking this
      (try
        (let [start-time (System/currentTimeMillis)]
          (loop []
            (let [^Buffer buffer (or (find-existing-buffer buffers block)
                                     (when-let [^Buffer unpinned (choose-unpinned-buffer buffers)]
                                       (.assignToBlock unpinned block)
                                       unpinned))]
              (if (nil? buffer)
                (if (> (- (System/currentTimeMillis) start-time) timeout)
                  (throw (ex-info "Buffer abort: waiting too long" {:block block}))
                  (do (.wait this timeout)
                      (recur)))
                (do
                  (when-not (.isPinned buffer)
                    (set! available (unchecked-dec-int available)))
                  (.pin buffer)
                  (.get buffers buffer)
                  buffer)))))
        (catch InterruptedException _
          (throw (ex-info "Buffer abort: interrupted" {:block block}))))))

  Object
  (toString [_]
    (str "BufferMgr[available=" available ", timeout=" timeout "ms]")))

(defn make-buffer-mgr
  [file-mgr log-mgr pool-size]
  (let [^LinkedHashMap lru-cache (lru/make-lru-cache pool-size nil)
        buffers (repeatedly pool-size #(make-buffer file-mgr log-mgr))]
    (doseq [buffer buffers]
      (.put lru-cache buffer buffer))
    (->BufferMgr lru-cache pool-size 10000)))
