(ns opusdb.log-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [opusdb.log :refer [make-log-mngr]]
            [opusdb.file :refer [make-file-mngr]])
  (:import [opusdb.log ILogMngr]))

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
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)]
      (is (instance? ILogMngr log-mngr))
      (is (= [] (.toVector log-mngr))))))

(deftest test-append-single-record
  (testing "Appending a single record"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)
          lsn (.append log-mngr (str->bytes "Test record 1"))]
      (is (= 1 lsn))
      (.flush log-mngr)
      (is (= 1 (count (.toVector log-mngr))))
      (is (= "Test record 1" (bytes->str (first (.toVector log-mngr))))))))

(deftest test-append-multiple-records
  (testing "Appending multiple records"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)
          records ["Record 1" "Record 2" "Record 3"]
          lsns (mapv #(.append log-mngr (str->bytes %)) records)]
      (is (= [1 2 3] lsns))
      (.flush log-mngr)
      (is (= (reverse records) (mapv bytes->str (.toVector log-mngr)))))))

(deftest test-flush-by-lsn
  (testing "Flushing when LSN threshold is met"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)
          lsn1 (.append log-mngr (str->bytes "Record 1"))
          lsn2 (.append log-mngr (str->bytes "Record 2"))]
      (.flush log-mngr 0)
      (.flush log-mngr lsn2)
      (is (= ["Record 2" "Record 1"] (mapv bytes->str (.toVector log-mngr)))))))

(deftest test-block-boundary
  (testing "Appending records that span multiple blocks"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)
          large-record (apply str (repeat 100 "X"))
          record-count 5
          lsns (repeatedly record-count #(.append log-mngr (str->bytes large-record)))]
      (is (= (range 1 (inc record-count)) lsns))
      (.flush log-mngr)
      (let [records (.toVector log-mngr)]
        (is (= record-count (count records)))
        (is (every? #(= large-record (bytes->str %)) records))))))

(deftest test-persistence
  (testing "Log persists across log manager instances"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          records ["First" "Second" "Third"]]
      (let [log-mngr1 (make-log-mngr file-mngr log-file)]
        (doseq [record records]
          (.append log-mngr1 (str->bytes record)))
        (.flush log-mngr1))
      (let [file-mngr2 (make-file-mngr test-dir block-size)
            log-mngr2 (make-log-mngr file-mngr2 log-file)]
        (is (= (reverse records) (mapv bytes->str (.toVector log-mngr2))))))))

(deftest test-empty-log
  (testing "Reading from empty log"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)]
      (is (= [] (.toVector log-mngr))))))

(deftest test-append-without-flush
  (testing "Records not visible until flushed"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)]
      (.append log-mngr (str->bytes "Not flushed"))
      (is (= 0 (count (.toVector log-mngr))))
      (.flush log-mngr)
      (is (= 1 (count (.toVector log-mngr))))
      (is (= "Not flushed" (bytes->str (first (.toVector log-mngr))))))))

(deftest test-concurrent-appends
  (testing "Thread-safe appending"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)
          thread-count 10
          records-per-thread 10
          futures (doall
                   (for [i (range thread-count)]
                     (future
                       (dotimes [j records-per-thread]
                         (.append log-mngr (str->bytes (str "Thread-" i "-Record-" j)))))))]
      (doseq [f futures] @f)
      (.flush log-mngr)
      (is (= (* thread-count records-per-thread) (count (.toVector log-mngr)))))))

(deftest test-binary-data
  (testing "Appending binary data"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)
          binary-data (byte-array (range -128 128))]
      (.append log-mngr binary-data)
      (.flush log-mngr)
      (is (= (seq binary-data) (seq (first (.toVector log-mngr))))))))

(deftest test-lsn-increments
  (testing "LSN increments correctly"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)
          lsn1 (.append log-mngr (str->bytes "First"))
          lsn2 (.append log-mngr (str->bytes "Second"))
          lsn3 (.append log-mngr (str->bytes "Third"))]
      (is (= 1 lsn1))
      (is (= 2 lsn2))
      (is (= 3 lsn3)))))

(deftest test-large-record
  (testing "Appending a record larger than block size boundary"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)
          large-record (apply str (repeat (- block-size 20) "A"))]
      (.append log-mngr (str->bytes large-record))
      (.flush log-mngr)
      (is (= 1 (count (.toVector log-mngr))))
      (is (= large-record (bytes->str (first (.toVector log-mngr))))))))

(deftest test-multiple-flushes
  (testing "Multiple flushes work correctly"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)]
      (.append log-mngr (str->bytes "First"))
      (.flush log-mngr)
      (.append log-mngr (str->bytes "Second"))
      (.flush log-mngr)
      (.append log-mngr (str->bytes "Third"))
      (.flush log-mngr)
      (is (= ["Third" "Second" "First"] (mapv bytes->str (.toVector log-mngr)))))))

(deftest test-flush-empty-log
  (testing "Flushing empty log doesn't cause errors"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)]
      (.flush log-mngr)
      (is (= [] (.toVector log-mngr))))))

(deftest test-record-ordering
  (testing "Records maintain insertion order in reverse"
    (let [log-file (unique-log-file)
          file-mngr (make-file-mngr test-dir block-size)
          log-mngr (make-log-mngr file-mngr log-file)
          records (mapv #(str "Record-" %) (range 20))]
      (doseq [record records]
        (.append log-mngr (str->bytes record)))
      (.flush log-mngr)
      (is (= (reverse records) (mapv bytes->str (.toVector log-mngr)))))))
