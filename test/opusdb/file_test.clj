(ns opusdb.file-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [opusdb.file :as file])
  (:import [java.nio ByteBuffer]
           [java.io File]))

(def test-db-path "test-db")
(def test-block-size 4096)

(defn delete-dir [path]
  (let [f (File. path)]
    (when (.exists f)
      (doseq [file (reverse (file-seq f))]
        (.delete file)))))

(use-fixtures :each
  (fn [f]
    (delete-dir test-db-path)
    (try (f)
         (finally (delete-dir test-db-path)))))

(deftest test-make-file-mngr
  (testing "creates file manager"
    (let [fm (file/make-file-mngr test-db-path test-block-size)]
      (is (= test-block-size (.blockSize fm)))
      (is (= test-db-path (:dbPath fm)))))

  (testing "creates directory"
    (file/make-file-mngr test-db-path test-block-size)
    (is (.exists (File. test-db-path))))

  (testing "cleans temp files"
    (file/make-file-mngr test-db-path test-block-size)
    (let [temp-file (File. test-db-path "temp_test.db")]
      (.createNewFile temp-file))
    (file/make-file-mngr test-db-path test-block-size)
    (is (not (.exists (File. test-db-path "temp_test.db"))))))

(deftest test-append
  (testing "appends single block"
    (let [fm (file/make-file-mngr test-db-path test-block-size)
          block (.appendNew fm "test.db")]
      (is (= "test.db" (:file-name block)))
      (is (= 0 (:blockn block)))))

  (testing "appends multiple blocks"
    (let [fm (file/make-file-mngr test-db-path test-block-size)
          b1 (.appendNew fm "test.db")
          b2 (.appendNew fm "test.db")
          b3 (.appendNew fm "test.db")
          actual (map :blockn [b1 b2 b3])]
      (is (= [1 2 3] actual))))

  (testing "appends to different files"
    (let [fm (file/make-file-mngr test-db-path test-block-size)
          b1 (.appendNew fm "file1.db")
          b2 (.appendNew fm "file2.db")]
      (is (= "file1.db" (:file-name b1)))
      (is (= "file2.db" (:file-name b2)))
      (is (= 0 (:blockn b1)))
      (is (= 0 (:blockn b2))))))

(deftest test-write-and-read
  (testing "single block"
    (let [fm (file/make-file-mngr test-db-path test-block-size)
          block (.appendNew fm "test.db")
          buf (ByteBuffer/allocate test-block-size)]
      (.put buf (byte-array [1 2 3 4 5]))
      (.writeTo fm block buf)

      (let [read-buf (ByteBuffer/allocate test-block-size)]
        (.readFrom fm block read-buf)
        (.rewind read-buf)
        (is (= [1 2 3 4 5] (take 5 (repeatedly #(.get read-buf))))))))

  (testing "multiple blocks"
    (let [fm (file/make-file-mngr test-db-path test-block-size)
          b1 (.appendNew fm "test.db")
          b2 (.appendNew fm "test.db")]
      (doto (ByteBuffer/allocate test-block-size)
        (.put (byte-array [10 20 30]))
        (as-> buf (.writeTo fm b1 buf)))
      (doto (ByteBuffer/allocate test-block-size)
        (.put (byte-array [40 50 60]))
        (as-> buf (.writeTo fm b2 buf)))

      (let [r1 (ByteBuffer/allocate test-block-size)
            r2 (ByteBuffer/allocate test-block-size)]
        (.readFrom fm b1 r1)
        (.readFrom fm b2 r2)
        (.rewind r1)
        (.rewind r2)
        (is (= [10 20 30] (take 3 (repeatedly #(.get r1)))))
        (is (= [40 50 60] (take 3 (repeatedly #(.get r2))))))))

  (testing "overwrite block"
    (let [fm (file/make-file-mngr test-db-path test-block-size)
          block (.appendNew fm "test.db")]
      (.writeTo fm block (doto (ByteBuffer/allocate test-block-size)
                           (.put (byte-array [1 2 3]))))
      (.writeTo fm block (doto (ByteBuffer/allocate test-block-size)
                           (.put (byte-array [7 8 9]))))

      (let [buf (ByteBuffer/allocate test-block-size)]
        (.readFrom fm block buf)
        (.rewind buf)
        (is (= [7 8 9] (take 3 (repeatedly #(.get buf)))))))))

(deftest test-multiple-files
  (testing "independent files"
    (let [fm (file/make-file-mngr test-db-path test-block-size)
          b1 (.appendNew fm "file1.db")
          b2 (.appendNew fm "file2.db")]
      (.writeTo fm b1 (doto (ByteBuffer/allocate test-block-size)
                        (.put (byte-array [100 101]))))
      (.writeTo fm b2 (doto (ByteBuffer/allocate test-block-size)
                        (.put (byte-array [50 51]))))
      (let [r1 (ByteBuffer/allocate test-block-size)
            r2 (ByteBuffer/allocate test-block-size)]
        (.readFrom fm b1 r1)
        (.readFrom fm b2 r2)
        (.rewind r1)
        (.rewind r2)
        (is (= (byte 100) (.get r1)))
        (is (= (byte 101) (.get r1)))
        (is (= (byte 50) (.get r2)))
        (is (= (byte 51) (.get r2)))))))

(deftest test-error-handling
  (testing "read large block id"
    (let [fm (file/make-file-mngr test-db-path test-block-size)
          _ (.appendNew fm "test.db")
          buf (ByteBuffer/allocate test-block-size)]
      (.readFrom fm {:file-name "test.db" :blockn 999} buf)
      (.rewind buf)
      (is (= 0 (.get buf)))))

  (testing "nil blockn throws"
    (let [fm (file/make-file-mngr test-db-path test-block-size)
          buf (ByteBuffer/allocate test-block-size)]
      (is (thrown? Exception
                   (.writeTo fm {:file-name "test.db" :blockn nil} buf))))))

(deftest test-block-alignment
  (testing "blocks aligned correctly"
    (let [fm (file/make-file-mngr test-db-path test-block-size)
          blocks (repeatedly 3 #(.appendNew fm "test.db"))]
      (doseq [[block val] (map vector blocks [11 22 33])]
        (.writeTo fm block (doto (ByteBuffer/allocate test-block-size)
                             (.put (byte-array [val])))))

      (doseq [[block expected] (map vector blocks [11 22 33])]
        (let [buf (ByteBuffer/allocate test-block-size)]
          (.readFrom fm block buf)
          (.rewind buf)
          (is (= expected (.get buf))))))))
