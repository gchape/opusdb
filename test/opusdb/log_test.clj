(ns opusdb.log-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [opusdb.log :as lm]
            [opusdb.file :refer [make-file-mgr]])
  (:import [opusdb.log LogMgr]))

(def test-dir "test-log-data")
(def block-size 400)

(defn- unique-log-file []
  (str "test-" (System/nanoTime) ".log"))

(defn setup-and-teardown [f]
  (let [dir (java.io.File. test-dir)]
    (when (.exists dir)
      (doseq [file (.listFiles dir)]
        (.delete file))
      (.delete dir))
    (.mkdirs dir))
  (f)
  (let [dir (java.io.File. test-dir)]
    (when (.exists dir)
      (doseq [file (.listFiles dir)]
        (.delete file))
      (.delete dir))))

(use-fixtures :each setup-and-teardown)

(defn- str->bytes [^String s]
  (.getBytes s "UTF-8"))

(defn- bytes->str [^bytes b]
  (String. b "UTF-8"))

(deftest test-create-log-manager
  (testing "Creating a new log manager"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)]
      (is (instance? LogMgr log-mgr))
      (is (= [] (seq log-mgr))))))

(deftest test-append-single-record
  (testing "Appending a single record"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)
          lsn (lm/append log-mgr (str->bytes "Test record 1"))]
      (is (= 1 lsn))
      (lm/flush log-mgr)
      (is (= 1 (count (seq log-mgr))))
      (is (= "Test record 1" (bytes->str (first (seq log-mgr))))))))

(deftest test-append-multiple-records
  (testing "Appending multiple records"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)
          records ["Record 1" "Record 2" "Record 3"]
          lsns (mapv #(lm/append log-mgr (str->bytes %)) records)]
      (is (= [1 2 3] lsns))
      (lm/flush log-mgr)
      (is (= records (mapv bytes->str (seq log-mgr)))))))

(deftest test-flush-by-lsn
  (testing "Flushing when LSN threshold is met"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)
          _ (lm/append log-mgr (str->bytes "Record 1"))
          lsn2 (lm/append log-mgr (str->bytes "Record 2"))]
      (lm/flush log-mgr 0)
      (lm/flush log-mgr lsn2)
      (is (= ["Record 1" "Record 2"] (mapv bytes->str (seq log-mgr)))))))

(deftest test-block-boundary
  (testing "Appending records that span multiple blocks"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)
          large-record (apply str (repeat 100 "X"))
          record-count 5
          lsns (repeatedly record-count #(lm/append log-mgr (str->bytes large-record)))]
      (is (= (range 1 (inc record-count)) lsns))
      (lm/flush log-mgr)
      (let [records (seq log-mgr)]
        (is (= record-count (count records)))
        (is (every? #(= large-record (bytes->str %)) records))))))

(deftest test-persistence
  (testing "Log persists across log manager instances"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          records ["First" "Second" "Third"]]
      (let [log-mgr1 (lm/make-log-mgr file-mgr log-file)]
        (doseq [record records]
          (lm/append log-mgr1 (str->bytes record)))
        (lm/flush log-mgr1))
      (let [file-mgr2 (make-file-mgr test-dir block-size)
            log-mgr2 (lm/make-log-mgr file-mgr2 log-file)]
        (is (= records (mapv bytes->str (seq log-mgr2))))))))

(deftest test-empty-log
  (testing "Reading from empty log"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)]
      (is (= [] (seq log-mgr))))))

(deftest test-append-without-flush
  (testing "Records not visible until flushed"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)]
      (lm/append log-mgr (str->bytes "Not flushed"))
      (is (= 0 (count (seq log-mgr))))
      (lm/flush log-mgr)
      (is (= 1 (count (seq log-mgr))))
      (is (= "Not flushed" (bytes->str (first (seq log-mgr))))))))

(deftest test-concurrent-appends
  (testing "Thread-safe appending"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)
          thread-count 10
          records-per-thread 10
          futures (doall
                   (for [i (range thread-count)]
                     (future
                       (dotimes [j records-per-thread]
                         (lm/append log-mgr (str->bytes (str "Thread-" i "-Record-" j)))))))]
      (doseq [f futures] @f)
      (lm/flush log-mgr)
      (is (= (* thread-count records-per-thread) (count (seq log-mgr)))))))

(deftest test-binary-data
  (testing "Appending binary data"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)
          binary-data (byte-array (range -128 128))]
      (lm/append log-mgr binary-data)
      (lm/flush log-mgr)
      (is (= (seq binary-data) (seq (first (seq log-mgr))))))))

(deftest test-lsn-increments
  (testing "LSN increments correctly"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)
          lsn1 (lm/append log-mgr (str->bytes "First"))
          lsn2 (lm/append log-mgr (str->bytes "Second"))
          lsn3 (lm/append log-mgr (str->bytes "Third"))]
      (is (= 1 lsn1))
      (is (= 2 lsn2))
      (is (= 3 lsn3)))))

(deftest test-large-record
  (testing "Appending a record larger than block size boundary"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)
          large-record (apply str (repeat (- block-size 20) "A"))]
      (lm/append log-mgr (str->bytes large-record))
      (lm/flush log-mgr)
      (is (= 1 (count (seq log-mgr))))
      (is (= large-record (bytes->str (first (seq log-mgr))))))))

(deftest test-multiple-flushes
  (testing "Multiple flushes work correctly"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)]
      (lm/append log-mgr (str->bytes "First"))
      (lm/flush log-mgr)
      (lm/append log-mgr (str->bytes "Second"))
      (lm/flush log-mgr)
      (lm/append log-mgr (str->bytes "Third"))
      (lm/flush log-mgr)
      (is (= ["First" "Second" "Third"] (mapv bytes->str (seq log-mgr)))))))

(deftest test-flush-empty-log
  (testing "Flushing empty log doesn't cause errors"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)]
      (lm/flush log-mgr)
      (is (= [] (seq log-mgr))))))

(deftest test-record-ordering
  (testing "Records maintain insertion order"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (lm/make-log-mgr file-mgr log-file)
          records (mapv #(str "Record-" %) (range 20))]
      (doseq [record records]
        (lm/append log-mgr (str->bytes record)))
      (lm/flush log-mgr)
      (is (= records (mapv bytes->str (seq log-mgr)))))))
