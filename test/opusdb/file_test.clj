(ns opusdb.file-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [opusdb.file :as file]
            [opusdb.page :as page])
  (:import [java.io File]))

(def test-db-dir "test-db")
(def test-block-size 4096)

(defn delete-dir [path]
  (let [file (File. path)]
    (when (.exists file)
      (doseq [file (reverse (file-seq file))]
        (.delete file)))))

(def test-counter (atom 0))

(defn unique-test-db-dir []
  (str test-db-dir "-" (swap! test-counter inc)))

(use-fixtures :once
  (fn [f]
    (try (f)
         (finally
           (doseq [i (range 1 (inc @test-counter))]
             (delete-dir (str test-db-dir "-" i)))))))

(deftest test-make-file-mgr
  (let [db-dir (unique-test-db-dir)]
    (testing "creates file manager"
      (let [fm (file/make-file-mgr db-dir test-block-size)]
        (is (= test-block-size (.blockSize fm)))
        (is (= db-dir (:db-dir fm)))))

    (testing "creates directory"
      (file/make-file-mgr db-dir test-block-size)
      (is (.exists (File. db-dir))))

    (testing "cleans temp files"
      (let [new-db-dir (unique-test-db-dir)
            _ (.mkdirs (File. new-db-dir))
            temp-file (File. new-db-dir "temp_should_be_deleted.db")]
        (.createNewFile temp-file)
        (is (.exists temp-file))
        (file/make-file-mgr new-db-dir test-block-size)
        (is (not (.exists temp-file)))))))

(deftest test-append
  (let [db-dir (unique-test-db-dir)]
    (testing "appends single block"
      (let [fm (file/make-file-mgr db-dir test-block-size)
            block (.append fm "test.db")]
        (is (= "test.db" (:file-name block)))
        (is (= 0 (:block-id block)))))

    (testing "appends multiple blocks"
      (let [fm (file/make-file-mgr db-dir test-block-size)
            b1 (.append fm "test.db")
            b2 (.append fm "test.db")
            b3 (.append fm "test.db")
            actual (map :block-id [b1 b2 b3])]
        (is (= [1 2 3] actual))))

    (testing "appends to different files"
      (let [fm (file/make-file-mgr db-dir test-block-size)
            b1 (.append fm "file1.db")
            b2 (.append fm "file2.db")]
        (is (= "file1.db" (:file-name b1)))
        (is (= "file2.db" (:file-name b2)))
        (is (= 0 (:block-id b1)))
        (is (= 0 (:block-id b2)))))))

(deftest test-write-and-read
  (let [db-dir (unique-test-db-dir)]
    (testing "single block"
      (let [fm (file/make-file-mgr db-dir test-block-size)
            block (.append fm "test.db")
            buf (page/make-page test-block-size)]
        (.setBytes buf 0 (byte-array [1 2 3 4 5]))
        (.write fm block buf)

        (let [read-buf (page/make-page test-block-size)]
          (.read fm block read-buf)
          (is (= [1 2 3 4 5] (seq (.getBytes read-buf 0)))))))

    (testing "multiple blocks"
      (let [fm (file/make-file-mgr db-dir test-block-size)
            b1 (.append fm "test.db")
            b2 (.append fm "test.db")
            buf1 (page/make-page test-block-size)
            buf2 (page/make-page test-block-size)]
        (.setBytes buf1 0 (byte-array [10 20 30]))
        (.write fm b1 buf1)
        (.setBytes buf2 0 (byte-array [40 50 60]))
        (.write fm b2 buf2)

        (let [r1 (page/make-page test-block-size)
              r2 (page/make-page test-block-size)]
          (.read fm b1 r1)
          (.read fm b2 r2)
          (is (= [10 20 30] (seq (.getBytes r1 0))))
          (is (= [40 50 60] (seq (.getBytes r2 0)))))))

    (testing "overwrite block"
      (let [fm (file/make-file-mgr db-dir test-block-size)
            block (.append fm "test.db")
            buf1 (page/make-page test-block-size)
            buf2 (page/make-page test-block-size)]
        (.setBytes buf1 0 (byte-array [1 2 3]))
        (.write fm block buf1)
        (.setBytes buf2 0 (byte-array [7 8 9]))
        (.write fm block buf2)

        (let [buf (page/make-page test-block-size)]
          (.read fm block buf)
          (is (= [7 8 9] (seq (.getBytes buf 0)))))))))

(deftest test-multiple-files
  (let [db-dir (unique-test-db-dir)]
    (testing "independent files"
      (let [fm (file/make-file-mgr db-dir test-block-size)
            b1 (.append fm "file1.db")
            b2 (.append fm "file2.db")
            buf1 (page/make-page test-block-size)
            buf2 (page/make-page test-block-size)]
        (.setBytes buf1 0 (byte-array [100 101]))
        (.write fm b1 buf1)
        (.setBytes buf2 0 (byte-array [50 51]))
        (.write fm b2 buf2)

        (let [r1 (page/make-page test-block-size)
              r2 (page/make-page test-block-size)]
          (.read fm b1 r1)
          (.read fm b2 r2)
          (let [bytes1 (.getBytes r1 0)
                bytes2 (.getBytes r2 0)]
            (is (= (byte 100) (aget bytes1 0)))
            (is (= (byte 101) (aget bytes1 1)))
            (is (= (byte 50) (aget bytes2 0)))
            (is (= (byte 51) (aget bytes2 1)))))))))

(deftest test-error-handling
  (let [db-dir (unique-test-db-dir)]
    (testing "read large block id"
      (let [fm (file/make-file-mgr db-dir test-block-size)
            _ (.append fm "test.db")
            buf (page/make-page test-block-size)]
        (.read fm {:file-name "test.db" :block-id 999} buf)
        (is (= 0 (.getInt buf 0)))))

    (testing "nil block-id throws"
      (let [fm (file/make-file-mgr db-dir test-block-size)
            buf (page/make-page test-block-size)]
        (is (thrown? Exception
                     (.write fm {:file-name "test.db" :block-id nil} buf)))))))

(deftest test-block-alignment
  (let [db-dir (unique-test-db-dir)]
    (testing "blocks aligned correctly"
      (let [fm (file/make-file-mgr db-dir test-block-size)
            blocks (take 3 (repeatedly #(.append fm "test.db")))]
        (doseq [[block val] (map vector blocks [11 22 33])]
          (let [buf (page/make-page test-block-size)]
            (.setBytes buf 0 (byte-array [val]))
            (.write fm block buf)))

        (doseq [[block expected] (map vector blocks [11 22 33])]
          (let [buf (page/make-page test-block-size)]
            (.read fm block buf)
            (let [bytes (.getBytes buf 0)]
              (is (= expected (aget bytes 0))))))))))
