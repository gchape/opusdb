(ns opusdb.log-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [opusdb.log :refer [make-log-mgr]]
            [opusdb.file :refer [make-file-mgr]])
  (:import [opusdb.log ILogMgr]))

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
          log-mgr (make-log-mgr file-mgr log-file)]
      (is (instance? ILogMgr log-mgr))
      (is (= [] (.getSnapshot log-mgr))))))

(deftest test-append-single-record
  (testing "Appending a single record"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)
          lsn (.append log-mgr (str->bytes "Test record 1"))]
      (is (= 1 lsn))
      (.flush log-mgr)
      (is (= 1 (count (.getSnapshot log-mgr))))
      (is (= "Test record 1" (bytes->str (first (.getSnapshot log-mgr))))))))

(deftest test-append-multiple-records
  (testing "Appending multiple records"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)
          records ["Record 1" "Record 2" "Record 3"]
          lsns (mapv #(.append log-mgr (str->bytes %)) records)]
      (is (= [1 2 3] lsns))
      (.flush log-mgr)
      (is (= (reverse records) (mapv bytes->str (.getSnapshot log-mgr)))))))

(deftest test-flush-by-lsn
  (testing "Flushing when LSN threshold is met"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)
          _ (.append log-mgr (str->bytes "Record 1"))
          lsn2 (.append log-mgr (str->bytes "Record 2"))]
      (.flush log-mgr 0)
      (.flush log-mgr lsn2)
      (is (= ["Record 2" "Record 1"] (mapv bytes->str (.getSnapshot log-mgr)))))))

(deftest test-block-boundary
  (testing "Appending records that span multiple blocks"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)
          large-record (apply str (repeat 100 "X"))
          record-count 5
          lsns (repeatedly record-count #(.append log-mgr (str->bytes large-record)))]
      (is (= (range 1 (inc record-count)) lsns))
      (.flush log-mgr)
      (let [records (.getSnapshot log-mgr)]
        (is (= record-count (count records)))
        (is (every? #(= large-record (bytes->str %)) records))))))

(deftest test-persistence
  (testing "Log persists across log manager instances"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          records ["First" "Second" "Third"]]
      (let [log-mgr1 (make-log-mgr file-mgr log-file)]
        (doseq [record records]
          (.append log-mgr1 (str->bytes record)))
        (.flush log-mgr1))
      (let [file-mgr2 (make-file-mgr test-dir block-size)
            log-mgr2 (make-log-mgr file-mgr2 log-file)]
        (is (= (reverse records) (mapv bytes->str (.getSnapshot log-mgr2))))))))

(deftest test-empty-log
  (testing "Reading from empty log"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)]
      (is (= [] (.getSnapshot log-mgr))))))

(deftest test-append-without-flush
  (testing "Records not visible until flushed"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)]
      (.append log-mgr (str->bytes "Not flushed"))
      (is (= 0 (count (.getSnapshot log-mgr))))
      (.flush log-mgr)
      (is (= 1 (count (.getSnapshot log-mgr))))
      (is (= "Not flushed" (bytes->str (first (.getSnapshot log-mgr))))))))

(deftest test-concurrent-appends
  (testing "Thread-safe appending"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)
          thread-count 10
          records-per-thread 10
          futures (doall
                   (for [i (range thread-count)]
                     (future
                       (dotimes [j records-per-thread]
                         (.append log-mgr (str->bytes (str "Thread-" i "-Record-" j)))))))]
      (doseq [f futures] @f)
      (.flush log-mgr)
      (is (= (* thread-count records-per-thread) (count (.getSnapshot log-mgr)))))))

(deftest test-binary-data
  (testing "Appending binary data"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)
          binary-data (byte-array (range -128 128))]
      (.append log-mgr binary-data)
      (.flush log-mgr)
      (is (= (seq binary-data) (seq (first (.getSnapshot log-mgr))))))))

(deftest test-lsn-increments
  (testing "LSN increments correctly"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)
          lsn1 (.append log-mgr (str->bytes "First"))
          lsn2 (.append log-mgr (str->bytes "Second"))
          lsn3 (.append log-mgr (str->bytes "Third"))]
      (is (= 1 lsn1))
      (is (= 2 lsn2))
      (is (= 3 lsn3)))))

(deftest test-large-record
  (testing "Appending a record larger than block size boundary"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)
          large-record (apply str (repeat (- block-size 20) "A"))]
      (.append log-mgr (str->bytes large-record))
      (.flush log-mgr)
      (is (= 1 (count (.getSnapshot log-mgr))))
      (is (= large-record (bytes->str (first (.getSnapshot log-mgr))))))))

(deftest test-multiple-flushes
  (testing "Multiple flushes work correctly"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)]
      (.append log-mgr (str->bytes "First"))
      (.flush log-mgr)
      (.append log-mgr (str->bytes "Second"))
      (.flush log-mgr)
      (.append log-mgr (str->bytes "Third"))
      (.flush log-mgr)
      (is (= ["Third" "Second" "First"] (mapv bytes->str (.getSnapshot log-mgr)))))))

(deftest test-flush-empty-log
  (testing "Flushing empty log doesn't cause errors"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)]
      (.flush log-mgr)
      (is (= [] (.getSnapshot log-mgr))))))

(deftest test-record-ordering
  (testing "Records maintain insertion order in reverse"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          log-mgr (make-log-mgr file-mgr log-file)
          records (mapv #(str "Record-" %) (range 20))]
      (doseq [record records]
        (.append log-mgr (str->bytes record)))
      (.flush log-mgr)
      (is (= (reverse records) (mapv bytes->str (.getSnapshot log-mgr)))))))
