(ns opusdb.memory.page-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [opusdb.memory.page :as p])
  (:import
   [java.nio ByteBuffer]))

(deftest test-make-page
  (testing "make-page with integer capacity"
    (let [buf (p/make-page 1024)]
      (is (instance? ByteBuffer buf))
      (is (= 1024 (.capacity buf)))))

  (testing "make-page with long capacity"
    (let [buf (p/make-page (long 2048))]
      (is (instance? ByteBuffer buf))
      (is (= 2048 (.capacity buf)))))

  (testing "make-page from byte array"
    (let [arr (byte-array 128)
          buf (p/make-page arr)]
      (is (instance? ByteBuffer buf))
      (is (= 128 (.capacity buf)))
      (is (= 0 (.get buf))))))

(deftest test-set-get-bytes
  (testing "store and retrieve bytes"
    (let [buf (p/make-page 128)
          data (byte-array [1 2 3 4 5])]
      (p/put-bytes buf 0 data)
      (is (= [1 2 3 4 5]
             (seq (p/get-bytes buf 0))))))

  (testing "bytes at different offsets"
    (let [buf (p/make-page 128)
          a (byte-array [10 20])
          b (byte-array [30 40 50])]
      (p/put-bytes buf 0 a)
      (p/put-bytes buf 16 b)

      (is (= [10 20] (seq (p/get-bytes buf 0))))
      (is (= [30 40 50] (seq (p/get-bytes buf 16)))))))

(deftest test-put-get-string
  (testing "store and retrieve ASCII string"
    (let [buf (p/make-page 128)]
      (p/put-string buf 0 "Hello")
      (is (= "Hello" (p/get-string buf 0)))))

  (testing "empty string"
    (let [buf (p/make-page 64)]
      (p/put-string buf 0 "")
      (is (= "" (p/get-string buf 0)))))

  (testing "string at offset"
    (let [buf (p/make-page 128)]
      (p/put-string buf 32 "World")
      (is (= "World" (p/get-string buf 32))))))

(deftest test-string-special-ascii
  (testing "ASCII punctuation"
    (let [buf (p/make-page 128)
          s "Hello@#$%123"]
      (p/put-string buf 0 s)
      (is (= s (p/get-string buf 0))))))

(deftest test-max-encoded-size
  (testing "empty string"
    (is (= 4 (p/max-encoded-size ""))))

  (testing "short string"
    (is (= 9 (p/max-encoded-size "Hello"))))

  (testing "longer string"
    (is (= 14 (p/max-encoded-size "HelloWorld")))))

(deftest test-page-independence
  (testing "buffers do not share state"
    (let [b1 (p/make-page 64)
          b2 (p/make-page 64)]
      (.putInt b1 0 100)
      (.putInt b2 0 200)

      (is (= 100 (.getInt b1 0)))
      (is (= 200 (.getInt b2 0))))))

(deftest test-overwrite-bytes
  (testing "overwriting replaces previous value"
    (let [buf (p/make-page 64)]
      (p/put-bytes buf 0 (byte-array [1 2 3]))
      (p/put-bytes buf 0 (byte-array [9 8]))

      (is (= [9 8]
             (seq (p/get-bytes buf 0)))))))

(deftest test-overwrite-string
  (testing "overwriting string replaces previous value"
    (let [buf (p/make-page 64)]
      (p/put-string buf 0 "First")
      (p/put-string buf 0 "Second")

      (is (= "Second" (p/get-string buf 0))))))

(deftest test-raw-bytebuf-access
  (testing "ByteBuf primitive operations still work"
    (let [buf (p/make-page 64)]
      (.putInt buf 0 42)
      (.putFloat buf 4 1.5)
      (.putDouble buf 8 3.25)

      (is (= 42 (.getInt buf 0)))
      (is (= 1.5 (.getFloat buf 4)))
      (is (= 3.25 (.getDouble buf 8))))))
