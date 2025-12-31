(ns opusdb.file-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [opusdb.file :as fm]
            [opusdb.page :as p])
  (:import [java.io File]))

(def test-db-dir "test-db")
(def test-block-size 4096)

;; ---------- helpers ----------
(defn delete-dir [path]
  (let [file (File. path)]
    (when (.exists file)
      (doseq [f (reverse (file-seq file))]
        (.delete f)))))

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

(defn read-bytes
  "Read n bytes from a ByteBuf starting at offset 0."
  [buf n]
  (let [arr (byte-array n)]
    (.getBytes buf 0 arr)
    (seq arr)))

;; ---------- tests ----------
(deftest test-make-file-mgr
  (let [db-dir (unique-test-db-dir)]
    (testing "creates file manager"
      (let [mgr (setup-fm db-dir)]
        (is (= test-block-size (fm/block-size mgr)))
        (is (= db-dir (:db-dir mgr)))))

    (testing "creates directory"
      (setup-fm db-dir)
      (is (.exists (File. db-dir))))

    (testing "cleans temp files"
      (let [dir (unique-test-db-dir)
            _ (.mkdirs (File. dir))
            temp (File. dir "temp_should_be_deleted.db")]
        (.createNewFile temp)
        (is (.exists temp))
        (setup-fm dir)
        (is (not (.exists temp)))))))

(deftest test-append
  (let [db-dir (unique-test-db-dir)]
    (testing "appends single block"
      (let [mgr (setup-fm db-dir)
            block (fm/append mgr "test.db")]
        (is (= "test.db" (:file-name block)))
        (is (= 0 (:index block)))))

    (testing "appends multiple blocks"
      (let [mgr (setup-fm db-dir)
            blocks (doall (repeatedly 3 #(fm/append mgr "test.db")))]
        (is (= [1 2 3] (map :index blocks)))))

    (testing "appends to different files"
      (let [mgr (setup-fm db-dir)
            b1 (fm/append mgr "file1.db")
            b2 (fm/append mgr "file2.db")]
        (is (= 0 (:index b1)))
        (is (= 0 (:index b2)))
        (is (= "file1.db" (:file-name b1)))
        (is (= "file2.db" (:file-name b2)))))))

(deftest test-write-and-read
  (let [db-dir (unique-test-db-dir)]
    (testing "single block write/read"
      (let [mgr (setup-fm db-dir)
            block (fm/append mgr "test.db")
            buf (p/make-page test-block-size)]
        (.setBytes buf 0 (byte-array [1 2 3 4 5]))
        (fm/write mgr block buf)

        (let [read-buf (p/make-page test-block-size)]
          (fm/read mgr block read-buf)
          (is (= [1 2 3 4 5] (read-bytes read-buf 5))))))

    (testing "multiple blocks"
      (let [mgr (setup-fm db-dir)
            b1 (fm/append mgr "test.db")
            b2 (fm/append mgr "test.db")
            buf1 (p/make-page test-block-size)
            buf2 (p/make-page test-block-size)]
        (.setBytes buf1 0 (byte-array [10 20 30]))
        (.setBytes buf2 0 (byte-array [40 50 60]))
        (fm/write mgr b1 buf1)
        (fm/write mgr b2 buf2)

        (let [r1 (p/make-page test-block-size)
              r2 (p/make-page test-block-size)]
          (fm/read mgr b1 r1)
          (fm/read mgr b2 r2)
          (is (= [10 20 30] (read-bytes r1 3)))
          (is (= [40 50 60] (read-bytes r2 3))))))

    (testing "overwrite block"
      (let [mgr (setup-fm db-dir)
            block (fm/append mgr "test.db")
            buf1 (p/make-page test-block-size)
            buf2 (p/make-page test-block-size)]
        (.setBytes buf1 0 (byte-array [1 2 3]))
        (fm/write mgr block buf1)
        (.setBytes buf2 0 (byte-array [7 8 9]))
        (fm/write mgr block buf2)

        (let [r (p/make-page test-block-size)]
          (fm/read mgr block r)
          (is (= [7 8 9] (read-bytes r 3))))))))

(deftest test-multiple-files
  (let [db-dir (unique-test-db-dir)]
    (testing "independent files"
      (let [mgr (setup-fm db-dir)
            b1 (fm/append mgr "file1.db")
            b2 (fm/append mgr "file2.db")
            buf1 (p/make-page test-block-size)
            buf2 (p/make-page test-block-size)]
        (.setBytes buf1 0 (byte-array [100 101]))
        (.setBytes buf2 0 (byte-array [50 51]))
        (fm/write mgr b1 buf1)
        (fm/write mgr b2 buf2)

        (let [r1 (p/make-page test-block-size)
              r2 (p/make-page test-block-size)]
          (fm/read mgr b1 r1)
          (fm/read mgr b2 r2)
          (is (= [100 101] (read-bytes r1 2)))
          (is (= [50 51] (read-bytes r2 2))))))))

(deftest test-block-alignment
  (let [db-dir (unique-test-db-dir)]
    (testing "blocks aligned correctly"
      (let [mgr (setup-fm db-dir)
            blocks (doall (repeatedly 3 #(fm/append mgr "test.db")))
            values [11 22 33]]
        (doseq [[block v] (map vector blocks values)]
          (let [buf (p/make-page test-block-size)]
            (.setBytes buf 0 (byte-array [v]))
            (fm/write mgr block buf)))

        (doseq [[block expected] (map vector blocks values)]
          (let [buf (p/make-page test-block-size)]
            (fm/read mgr block buf)
            (is (= expected (first (read-bytes buf 1))))))))))
