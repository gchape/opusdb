(ns opusdb.page-test
  (:require [clojure.test :refer [deftest testing is]]
            [opusdb.page :as page])
  (:import [io.netty.buffer ByteBuf]))

;; ---------- make-page ---------- 
(deftest test-make-page
  (testing "make-page with integer capacity"
    (let [buf (page/make-page 1024)]
      (is (instance? ByteBuf buf))
      (is (= 1024 (.capacity buf)))))

  (testing "make-page with long capacity"
    (let [buf (page/make-page (long 2048))]
      (is (instance? ByteBuf buf))
      (is (= 2048 (.capacity buf)))))

  (testing "make-page from byte array"
    (let [arr (byte-array 128)
          buf (page/make-page arr)]
      (is (instance? ByteBuf buf))
      (is (= 128 (.capacity buf)))
      (is (= 0 (.getByte buf 0))))))

;; ---------- bytes ---------- 
(deftest test-set-get-bytes
  (testing "store and retrieve bytes"
    (let [buf (page/make-page 128)
          data (byte-array [1 2 3 4 5])]
      (page/set-bytes buf 0 data)
      (is (= [1 2 3 4 5]
             (seq (page/get-bytes buf 0))))))

  (testing "bytes at different offsets"
    (let [buf (page/make-page 128)
          a (byte-array [10 20])
          b (byte-array [30 40 50])]
      (page/set-bytes buf 0 a)
      (page/set-bytes buf 16 b)

      (is (= [10 20] (seq (page/get-bytes buf 0))))
      (is (= [30 40 50] (seq (page/get-bytes buf 16)))))))

;; ---------- strings ---------- 
(deftest test-set-get-string
  (testing "store and retrieve ASCII string"
    (let [buf (page/make-page 128)]
      (page/set-string buf 0 "Hello")
      (is (= "Hello" (page/get-string buf 0)))))

  (testing "empty string"
    (let [buf (page/make-page 64)]
      (page/set-string buf 0 "")
      (is (= "" (page/get-string buf 0)))))

  (testing "string at offset"
    (let [buf (page/make-page 128)]
      (page/set-string buf 32 "World")
      (is (= "World" (page/get-string buf 32))))))

(deftest test-string-special-ascii
  (testing "ASCII punctuation"
    (let [buf (page/make-page 128)
          s "Hello@#$%123"]
      (page/set-string buf 0 s)
      (is (= s (page/get-string buf 0))))))

;; ---------- max-encoded-size ---------- 
(deftest test-max-encoded-size
  (testing "empty string"
    (is (= 4 (page/max-encoded-size ""))))

  (testing "short string"
    ;; 5 ASCII chars + 4-byte length prefix
    (is (= 9 (page/max-encoded-size "Hello"))))

  (testing "longer string"
    (is (= 14 (page/max-encoded-size "HelloWorld")))))

;; ---------- independence ---------- 
(deftest test-page-independence
  (testing "buffers do not share state"
    (let [b1 (page/make-page 64)
          b2 (page/make-page 64)]
      (.setInt b1 0 100)
      (.setInt b2 0 200)

      (is (= 100 (.getInt b1 0)))
      (is (= 200 (.getInt b2 0))))))

;; ---------- overwrite behavior ---------- 
(deftest test-overwrite-bytes
  (testing "overwriting replaces previous value"
    (let [buf (page/make-page 64)]
      (page/set-bytes buf 0 (byte-array [1 2 3]))
      (page/set-bytes buf 0 (byte-array [9 8]))

      (is (= [9 8]
             (seq (page/get-bytes buf 0)))))))

(deftest test-overwrite-string
  (testing "overwriting string replaces previous value"
    (let [buf (page/make-page 64)]
      (page/set-string buf 0 "First")
      (page/set-string buf 0 "Second")

      (is (= "Second" (page/get-string buf 0))))))

;; ---------- raw ByteBuf semantics (sanity) ---------- 
(deftest test-raw-bytebuf-access
  (testing "ByteBuf primitive operations still work"
    (let [buf (page/make-page 64)]
      (.setInt buf 0 42)
      (.setFloat buf 4 1.5)
      (.setDouble buf 8 3.25)

      (is (= 42 (.getInt buf 0)))
      (is (= 1.5 (.getFloat buf 4)))
      (is (= 3.25 (.getDouble buf 8))))))
