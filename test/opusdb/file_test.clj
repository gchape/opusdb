(ns opusdb.file-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [opusdb.file :as fm]
            [opusdb.page :as p])
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
    (try
      (f)
      (finally
        (doseq [i (range 1 (inc @test-counter))]
          (delete-dir (str test-db-dir "-" i)))))))

(defn setup-fm [db-dir]
  (fm/make-file-mgr db-dir test-block-size))

(deftest test-make-file-mgr
  (let [db-dir (unique-test-db-dir)]
    (testing "creates file manager"
      (let [fm (setup-fm db-dir)]
        (is (= test-block-size (fm/block-size fm)))
        (is (= db-dir (:db-dir fm)))))

    (testing "creates directory"
      (setup-fm db-dir)
      (is (.exists (File. db-dir))))

    (testing "cleans temp files"
      (let [new-db-dir (unique-test-db-dir)
            _ (.mkdirs (File. new-db-dir))
            temp-file (File. new-db-dir "temp_should_be_deleted.db")]
        (.createNewFile temp-file)
        (is (.exists temp-file))
        (setup-fm new-db-dir)
        (is (not (.exists temp-file)))))))

(deftest test-append
  (let [db-dir (unique-test-db-dir)]
    (testing "appends single block"
      (let [fm (setup-fm db-dir)
            block (fm/append fm "test.db")]
        (is (= "test.db" (:file-name block)))
        (is (= 0 (:index block)))))

    (testing "appends multiple blocks"
      (let [fm (setup-fm db-dir)
            blocks (map (fn* [_] (fm/append fm "test.db")) (range 3))]
        (is (= [1 2 3] (map :index blocks)))))

    (testing "appends to different files"
      (let [fm (setup-fm db-dir)
            block-id1 (fm/append fm "file1.db")
            block-id2 (fm/append fm "file2.db")]
        (is (= "file1.db" (:file-name block-id1)))
        (is (= "file2.db" (:file-name block-id2)))
        (is (= 0 (:index block-id1)))
        (is (= 0 (:index block-id2)))))))

(deftest test-write-and-read
  (let [db-dir (unique-test-db-dir)]
    (testing "single block"
      (let [fm (setup-fm db-dir)
            block (fm/append fm "test.db")
            buf (p/make-page test-block-size)]
        (.setBytes buf 0 (byte-array [1 2 3 4 5]))
        (fm/write fm block buf)

        (let [read-buf (p/make-page test-block-size)]
          (fm/read fm block read-buf)
          (is (= [1 2 3 4 5] (seq (.getBytes read-buf 0)))))))

    (testing "multiple blocks"
      (let [fm (setup-fm db-dir)
            block-id1 (fm/append fm "test.db")
            block-id2 (fm/append fm "test.db")
            buffer1 (p/make-page test-block-size)
            buffer2 (p/make-page test-block-size)]
        (.setBytes buffer1 0 (byte-array [10 20 30]))
        (fm/write fm block-id1 buffer1)
        (.setBytes buffer2 0 (byte-array [40 50 60]))
        (fm/write fm block-id2 buffer2)

        (let [r1 (p/make-page test-block-size)
              r2 (p/make-page test-block-size)]
          (fm/read fm block-id1 r1)
          (fm/read fm block-id2 r2)
          (is (= [10 20 30] (seq (.getBytes r1 0))))
          (is (= [40 50 60] (seq (.getBytes r2 0)))))))

    (testing "overwrite block"
      (let [fm (setup-fm db-dir)
            block (fm/append fm "test.db")
            buffer1 (p/make-page test-block-size)
            buffer2 (p/make-page test-block-size)]
        (.setBytes buffer1 0 (byte-array [1 2 3]))
        (fm/write fm block buffer1)
        (.setBytes buffer2 0 (byte-array [7 8 9]))
        (fm/write fm block buffer2)

        (let [buf (p/make-page test-block-size)]
          (fm/read fm block buf)
          (is (= [7 8 9] (seq (.getBytes buf 0)))))))))

(deftest test-multiple-files
  (let [db-dir (unique-test-db-dir)]
    (testing "independent files"
      (let [fm (setup-fm db-dir)
            block-id1 (fm/append fm "file1.db")
            block-id2 (fm/append fm "file2.db")
            buffer1 (p/make-page test-block-size)
            buffer2 (p/make-page test-block-size)]
        (.setBytes buffer1 0 (byte-array [100 101]))
        (fm/write fm block-id1 buffer1)
        (.setBytes buffer2 0 (byte-array [50 51]))
        (fm/write fm block-id2 buffer2)

        (let [r1 (p/make-page test-block-size)
              r2 (p/make-page test-block-size)]
          (fm/read fm block-id1 r1)
          (fm/read fm block-id2 r2)
          (is (= (byte 100) (aget (.getBytes r1 0) 0)))
          (is (= (byte 101) (aget (.getBytes r1 0) 1)))
          (is (= (byte 50) (aget (.getBytes r2 0) 0)))
          (is (= (byte 51) (aget (.getBytes r2 0) 1))))))))

(deftest test-error-handling
  (let [db-dir (unique-test-db-dir)]
    (testing "read large block id"
      (let [fm (setup-fm db-dir)
            _ (fm/append fm "test.db")
            buffer (p/make-page test-block-size)]
        (fm/read fm {:file-name "test.db" :index 999} buffer)
        (is (= 0 (.getInt buffer 0)))))

    (testing "nil block-id throws"
      (let [fm (setup-fm db-dir)
            buffer (p/make-page test-block-size)]
        (is (thrown? Exception
                     (fm/write fm {:file-name "test.db" :index nil} buffer)))))))

(deftest test-block-alignment
  (let [db-dir (unique-test-db-dir)]
    (testing "blocks aligned correctly"
      (let [fm (setup-fm db-dir)
            blocks (take 3 (repeatedly #(fm/append fm "test.db")))]
        (doseq [[block val] (map vector blocks [11 22 33])]
          (let [buffer (p/make-page test-block-size)]
            (.setBytes buffer 0 (byte-array [val]))
            (fm/write fm block buffer)))

        (doseq [[block expected] (map vector blocks [11 22 33])]
          (let [buf (p/make-page test-block-size)]
            (fm/read fm block buf)
            (is (= expected (aget (.getBytes buf 0) 0)))))))))
