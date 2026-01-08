(ns opusdb.page
  (:import
   [java.nio ByteBuffer]
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
  [^ByteBuffer buf offset]
  (let [length (.getInt buf offset)
        ^bytes bytearr (byte-array length)]
    (.position buf ^int (+ offset 4))
    (.get buf bytearr)
    bytearr))

(defn put-bytes
  [^ByteBuffer buf offset ^bytes b]
  (let [length (alength b)]
    (.putInt buf offset length)
    (.position buf ^int (+ offset 4))
    (.put buf b)))

(defn get-string
  [^ByteBuffer buf offset]
  (String. ^bytes (get-bytes buf offset) charset))

(defn put-string
  [^ByteBuffer buf offset ^String s]
  (let [^bytes bytearr (.getBytes s charset)]
    (put-bytes buf offset bytearr)))

(defmulti make-page class)

(defmethod make-page Integer
  [capacity]
  (ByteBuffer/allocate capacity))

(defmethod make-page Long
  [capacity]
  (ByteBuffer/allocate (int capacity)))

(defmethod make-page (class (byte-array 0))
  [^bytes bytearr]
  (ByteBuffer/wrap bytearr))
