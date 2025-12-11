(ns opusdb.file-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [opusdb.file :as file]
            [opusdb.page :as page])
  (:import [java.io File]))

(def test-db-path "test-db")
(def test-block-size 4096)

(defn delete-dir [path]
  (let [f (File. path)]
    (when (.exists f)
      (doseq [file (reverse (file-seq f))]
        (.delete file)))))

(def test-counter (atom 0))

(defn unique-test-db-path []
  (str test-db-path "-" (swap! test-counter inc)))

(use-fixtures :once
  (fn [f]
    (try (f)
         (finally 
           (doseq [i (range 1 (inc @test-counter))]
             (delete-dir (str test-db-path "-" i)))))))

(deftest test-make-file-mngr
  (let [db-path (unique-test-db-path)]
    (testing "creates file manager"
      (let [fm (file/make-file-mngr db-path test-block-size)]
        (is (= test-block-size (.blockSize fm)))
        (is (= db-path (:db-path fm)))))

    (testing "creates directory"
      (file/make-file-mngr db-path test-block-size)
      (is (.exists (File. db-path))))

    (testing "cleans temp files"
      (let [new-db-path (unique-test-db-path)
            _ (.mkdirs (File. new-db-path))
            temp-file (File. new-db-path "temp_should_be_deleted.db")]
        (.createNewFile temp-file)
        (is (.exists temp-file))
        (file/make-file-mngr new-db-path test-block-size)
        (is (not (.exists temp-file)))))))

(deftest test-append
  (let [db-path (unique-test-db-path)]
    (testing "appends single block"
      (let [fm (file/make-file-mngr db-path test-block-size)
            block (.append fm "test.db")]
        (is (= "test.db" (:file-name block)))
        (is (= 0 (:blockn block)))))

    (testing "appends multiple blocks"
      (let [fm (file/make-file-mngr db-path test-block-size)
            b1 (.append fm "test.db")
            b2 (.append fm "test.db")
            b3 (.append fm "test.db")
            actual (map :blockn [b1 b2 b3])]
        (is (= [1 2 3] actual))))

    (testing "appends to different files"
      (let [fm (file/make-file-mngr db-path test-block-size)
            b1 (.append fm "file1.db")
            b2 (.append fm "file2.db")]
        (is (= "file1.db" (:file-name b1)))
        (is (= "file2.db" (:file-name b2)))
        (is (= 0 (:blockn b1)))
        (is (= 0 (:blockn b2)))))))

(deftest test-write-and-read
  (let [db-path (unique-test-db-path)]
    (testing "single block"
      (let [fm (file/make-file-mngr db-path test-block-size)
            block (.append fm "test.db")
            buf (page/make-page test-block-size)]
        (.setBytes buf 0 (byte-array [1 2 3 4 5]))
        (.writeTo fm block buf)

        (let [read-buf (page/make-page test-block-size)]
          (.readFrom fm block read-buf)
          (is (= [1 2 3 4 5] (seq (.getBytes read-buf 0)))))))

    (testing "multiple blocks"
      (let [fm (file/make-file-mngr db-path test-block-size)
            b1 (.append fm "test.db")
            b2 (.append fm "test.db")
            buf1 (page/make-page test-block-size)
            buf2 (page/make-page test-block-size)]
        (.setBytes buf1 0 (byte-array [10 20 30]))
        (.writeTo fm b1 buf1)
        (.setBytes buf2 0 (byte-array [40 50 60]))
        (.writeTo fm b2 buf2)

        (let [r1 (page/make-page test-block-size)
              r2 (page/make-page test-block-size)]
          (.readFrom fm b1 r1)
          (.readFrom fm b2 r2)
          (is (= [10 20 30] (seq (.getBytes r1 0))))
          (is (= [40 50 60] (seq (.getBytes r2 0)))))))

    (testing "overwrite block"
      (let [fm (file/make-file-mngr db-path test-block-size)
            block (.append fm "test.db")
            buf1 (page/make-page test-block-size)
            buf2 (page/make-page test-block-size)]
        (.setBytes buf1 0 (byte-array [1 2 3]))
        (.writeTo fm block buf1)
        (.setBytes buf2 0 (byte-array [7 8 9]))
        (.writeTo fm block buf2)

        (let [buf (page/make-page test-block-size)]
          (.readFrom fm block buf)
          (is (= [7 8 9] (seq (.getBytes buf 0)))))))))

(deftest test-multiple-files
  (let [db-path (unique-test-db-path)]
    (testing "independent files"
      (let [fm (file/make-file-mngr db-path test-block-size)
            b1 (.append fm "file1.db")
            b2 (.append fm "file2.db")
            buf1 (page/make-page test-block-size)
            buf2 (page/make-page test-block-size)]
        (.setBytes buf1 0 (byte-array [100 101]))
        (.writeTo fm b1 buf1)
        (.setBytes buf2 0 (byte-array [50 51]))
        (.writeTo fm b2 buf2)

        (let [r1 (page/make-page test-block-size)
              r2 (page/make-page test-block-size)]
          (.readFrom fm b1 r1)
          (.readFrom fm b2 r2)
          (let [bytes1 (.getBytes r1 0)
                bytes2 (.getBytes r2 0)]
            (is (= (byte 100) (aget bytes1 0)))
            (is (= (byte 101) (aget bytes1 1)))
            (is (= (byte 50) (aget bytes2 0)))
            (is (= (byte 51) (aget bytes2 1)))))))))

(deftest test-error-handling
  (let [db-path (unique-test-db-path)]
    (testing "read large block id"
      (let [fm (file/make-file-mngr db-path test-block-size)
            _ (.append fm "test.db")
            buf (page/make-page test-block-size)]
        (.readFrom fm {:file-name "test.db" :blockn 999} buf)
        (is (= 0 (.getInt buf 0)))))

    (testing "nil blockn throws"
      (let [fm (file/make-file-mngr db-path test-block-size)
            buf (page/make-page test-block-size)]
        (is (thrown? Exception
                     (.writeTo fm {:file-name "test.db" :blockn nil} buf)))))))

(deftest test-block-alignment
  (let [db-path (unique-test-db-path)]
    (testing "blocks aligned correctly"
      (let [fm (file/make-file-mngr db-path test-block-size)
            blocks (take 3 (repeatedly #(.append fm "test.db")))]
        (doseq [[block val] (map vector blocks [11 22 33])]
          (let [buf (page/make-page test-block-size)]
            (.setBytes buf 0 (byte-array [val]))
            (.writeTo fm block buf)))

        (doseq [[block expected] (map vector blocks [11 22 33])]
          (let [buf (page/make-page test-block-size)]
            (.readFrom fm block buf)
            (let [bytes (.getBytes buf 0)]
              (is (= expected (aget bytes 0))))))))))
