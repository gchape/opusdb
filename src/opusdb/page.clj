(ns opusdb.page
  (:refer-clojure :exclude [+ *])
  (:import [java.nio ByteBuffer]
           [java.nio.charset Charset]))

(def +
  (fn* [x y] (unchecked-add-int x y)))

(def *
  (fn [x y] (unchecked-multiply-int x y)))

(def ^Charset charset
  (Charset/forName "US-ASCII"))

(defn max-encoded-size
  [^String s]
  (+ 4
     (* (count s)
        (-> (.newEncoder charset)
            (.maxBytesPerChar)
            (int)))))

(definterface IPage
  (^java.nio.ByteBuffer rewind [])

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

  (^bytes getBytes [^int offset])
  (^void setBytes [^int offset ^bytes b])

  (^String getString [^int offset])
  (^void setString [^int offset ^String s]))

(defrecord Page [^ByteBuffer bb ^Charset charset]
  IPage
  (rewind [_]
    (do (.rewind bb) bb))

  (getShort [_ offset]
    (.getShort bb offset))
  (setShort [_ offset n]
    (.putShort bb offset n))

  (getInt [_ offset]
    (.getInt bb offset))
  (setInt [_ offset n]
    (.putInt bb offset n))

  (getLong [_ offset]
    (.getLong bb offset))
  (setLong [_ offset l]
    (.putLong bb offset l))

  (getFloat [_ offset]
    (.getFloat bb offset))
  (setFloat [_ offset f]
    (.putFloat bb offset f))

  (getDouble [_ offset]
    (.getDouble bb offset))
  (setDouble [_ offset d]
    (.putDouble bb offset d))

  (getBytes [_ offset]
    (let [length (.getInt bb offset)
          b (byte-array length)
          original-pos (.position bb)]
      (.position bb ^int (+ 4 offset))
      (.get bb b)
      (.position bb original-pos)
      b))

  (setBytes [_ offset b]
    (let [length (alength b)]
      (.position bb offset)
      (.putInt bb length)
      (.put bb b)))

  (getString [_ offset]
    (let [length (.getInt bb offset)
          b (byte-array length)
          original-pos (.position bb)]
      (.position bb ^int (+ 4 offset))
      (.get bb b)
      (.position bb original-pos)
      (String. b charset)))

  (setString [_ offset s]
    (let [b (.getBytes s charset)
          length (alength b)]
      (.position bb offset)
      (.putInt bb length)
      (.put bb b))))

(defmulti make-page
  class)

(defmethod make-page Long
  [block-size]
  (Page. (ByteBuffer/allocate block-size)
         charset))

(defmethod make-page Integer
  [block-size]
  (Page. (ByteBuffer/allocate block-size)
         charset))

(defmethod make-page (class (byte-array 0))
  [byte-arr]
  (Page. (ByteBuffer/wrap byte-arr)
         charset))
