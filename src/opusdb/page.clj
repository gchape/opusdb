(ns opusdb.page
  (:import [java.nio ByteBuffer]
           [java.nio.charset Charset]))

(def ^Charset charset (Charset/forName "US-ASCII"))

(defn max-encoded-size
  "Calculates the maximum number of bytes required to store a string in the page.
  Includes 4 bytes for the length prefix plus the maximum possible encoded size
  of the string content based on the charset's maximum bytes per character."
  [^String s]
  (+ 4
     (* (count s)
        (-> (.newEncoder charset)
            (.maxBytesPerChar)
            (int)))))

(definterface IPage
  (^short getShort [^int offset])
  (^void setShort [^int offset ^short n])

  (^int getInt [^int offset])
  (^void setInt [^int offset ^int n])

  (^long getLong [^int offset])
  (^void setLong [^int offset ^long l])

  (^float getFloat [^int offset])
  (^void setFloat [^int offset ^float f])

  (^double getDouble [^int offset])
  (^void setDouble [^int offset ^double d])

  (^String getString [^int offset])
  (^void setString [^int offset ^String s]))

(definterface IBuffer
  (^void rewind []))

(defrecord Page [^ByteBuffer bb ^Charset charset]
  IBuffer
  (rewind [_] (.rewind bb))

  IPage
  (getShort [_ offset] (.getShort bb offset))
  (setShort [_ offset n] (.putShort bb offset n))

  (getInt [_ offset] (.getInt bb offset))
  (setInt [_ offset n] (.putInt bb offset n))

  (getLong [_ offset] (.getLong bb offset))
  (setLong [_ offset l] (.putLong bb offset l))

  (getFloat [_ offset] (.getFloat bb offset))
  (setFloat [_ offset f] (.putFloat bb offset f))

  (getDouble [_ offset] (.getDouble bb offset))
  (setDouble [_ offset d] (.putDouble bb offset d))

  (getString [_ offset]
    (let [length (.getInt bb offset)
          b (byte-array length)
          original-pos (.position bb)]
      (.position bb (+ 4 offset))
      (.get bb b)
      (.position bb original-pos)
      (String. b charset)))

  (setString [_ offset s]
    (let [b (.getBytes s charset)
          length (alength b)]
      (.putInt bb offset length)
      (.position bb (+ 4 offset))
      (.put bb b))))

(defmulti make-page
  "Creates a Page instance from either a block size (Long) or byte array.
  Dispatches on the string representation of the argument's type."
  class)

(defmethod make-page Long
  [block-size]
  "Creates a new Page with an allocated ByteBuffer of the specified block-size.
  Uses US-ASCII charset for string encoding."
  (Page. (ByteBuffer/allocate block-size)
         charset))

(defmethod make-page (class
                      (byte-array 0))
  [b]
  "Creates a new Page by wrapping an existing byte array.
  Uses US-ASCII charset for string encoding."
  (Page. (ByteBuffer/wrap b)
         charset))
