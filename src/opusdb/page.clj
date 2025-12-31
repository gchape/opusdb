(ns opusdb.page
  (:import [io.netty.buffer ByteBuf Unpooled PooledByteBufAllocator]
           [java.nio.charset Charset StandardCharsets]))

(def ^Charset charset StandardCharsets/US_ASCII)

(defn max-encoded-size
  [^String s]
  (+ 4
     (* (count s)
        (-> (.newEncoder charset)
            (.maxBytesPerChar)
            (int)))))

(defn get-bytes
  [^ByteBuf buf offset]
  (let [length (.getInt buf offset)
        bytearr (byte-array length)]
    (.getBytes buf (unchecked-add-int 4 offset) bytearr)
    bytearr))

(defn set-bytes
  [^ByteBuf buf offset ^bytes b]
  (let [length (alength b)]
    (.setInt buf offset length)
    (.setBytes buf (unchecked-add-int 4 offset) b)))

(defn get-string
  [^ByteBuf buf offset]
  (let [length (.getInt buf offset)
        bytearr (byte-array length)]
    (.getBytes buf (unchecked-add-int 4 offset) bytearr)
    (String. bytearr charset)))

(defn set-string
  [^ByteBuf buf offset ^String s]
  (let [bytearr (.getBytes s charset)
        length (alength bytearr)]
    (.setInt buf offset length)
    (.setBytes buf (unchecked-add-int 4 offset) bytearr)))

(defmulti make-page class)

(defmethod make-page Integer
  [capacity]
  (.heapBuffer PooledByteBufAllocator/DEFAULT capacity))

(defmethod make-page Long
  [capacity]
  (.heapBuffer PooledByteBufAllocator/DEFAULT capacity))

(defmethod make-page (class (byte-array 0))
  [^bytes bytearr]
  (Unpooled/wrappedBuffer bytearr))
