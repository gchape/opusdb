(ns opusdb.log-mgr-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [opusdb.file-mgr :refer [make-file-mgr]]
   [opusdb.log-mgr :as lm])
  (:import
   [opusdb.log_mgr LogMgr]))

(def test-dir "test-log-data")
(def block-size 400)

(defn- unique-log-file []
  (str "test-" (System/nanoTime) ".log"))

(defn- str->bytes [^String s]
  (.getBytes s "UTF-8"))

(defn- bytes->str [^bytes b]
  (String. b "UTF-8"))

(defn- clean-test-dir []
  (let [dir (java.io.File. test-dir)]
    (when (.exists dir)
      (doseq [file (.listFiles dir)]
        (.delete file))
      (.delete dir))))

(defn- setup-and-teardown [f]
  (clean-test-dir)
  (.mkdirs (java.io.File. test-dir))
  (f)
  (clean-test-dir))

(use-fixtures :each setup-and-teardown)

(defn- make-test-log-mgr
  "Create a log manager with unique log file for testing."
  []
  (let [log-file (unique-log-file)
        file-mgr (make-file-mgr test-dir block-size)]
    [(lm/make-log-mgr file-mgr log-file) log-file file-mgr]))

(defn- append-records
  "Append multiple string records to log manager."
  [log-mgr records]
  (mapv #(lm/append! log-mgr (str->bytes %)) records))

(defn- read-records
  "Read all records from log manager as strings."
  [log-mgr]
  (mapv bytes->str (seq log-mgr)))

(deftest test-create-log-manager
  (testing "Creating a new log manager"
    (let [[log-mgr] (make-test-log-mgr)]
      (is (instance? LogMgr log-mgr))
      (is (empty? (seq log-mgr))))))

(deftest test-empty-log
  (testing "Reading from empty log returns empty sequence"
    (let [[log-mgr] (make-test-log-mgr)]
      (is (= [] (seq log-mgr))))))

(deftest test-flush-empty-log
  (testing "Flushing empty log doesn't cause errors"
    (let [[log-mgr] (make-test-log-mgr)]
      (lm/flush! log-mgr)
      (is (empty? (seq log-mgr))))))

(deftest test-append-single-record
  (testing "Appending and retrieving a single record"
    (let [[log-mgr] (make-test-log-mgr)
          lsn (lm/append! log-mgr (str->bytes "Test record 1"))]
      (is (= 1 lsn))
      (lm/flush! log-mgr)
      (is (= ["Test record 1"] (read-records log-mgr))))))

(deftest test-append-without-flush
  (testing "Records not visible until flushed"
    (let [[log-mgr] (make-test-log-mgr)]
      (lm/append! log-mgr (str->bytes "Not flushed"))
      (is (empty? (seq log-mgr)))
      (lm/flush! log-mgr)
      (is (= ["Not flushed"] (read-records log-mgr))))))

(deftest test-append-multiple-records
  (testing "Appending multiple records maintains order"
    (let [[log-mgr] (make-test-log-mgr)
          records ["Record 1" "Record 2" "Record 3"]
          lsns (append-records log-mgr records)]
      (is (= [1 2 3] lsns))
      (lm/flush! log-mgr)
      (is (= records (read-records log-mgr))))))

(deftest test-record-ordering
  (testing "Records maintain insertion order with many records"
    (let [[log-mgr] (make-test-log-mgr)
          records (mapv #(str "Record-" %) (range 20))]
      (append-records log-mgr records)
      (lm/flush! log-mgr)
      (is (= records (read-records log-mgr))))))

(deftest test-lsn-increments
  (testing "LSN increments correctly for each append"
    (let [[log-mgr] (make-test-log-mgr)
          lsn1 (lm/append! log-mgr (str->bytes "First"))
          lsn2 (lm/append! log-mgr (str->bytes "Second"))
          lsn3 (lm/append! log-mgr (str->bytes "Third"))]
      (is (= 1 lsn1))
      (is (= 2 lsn2))
      (is (= 3 lsn3)))))

(deftest test-flush-by-lsn
  (testing "Conditional flushing based on LSN threshold"
    (let [[log-mgr] (make-test-log-mgr)
          _ (lm/append! log-mgr (str->bytes "Record 1"))
          lsn2 (lm/append! log-mgr (str->bytes "Record 2"))]
      (lm/flush! log-mgr 0)
      (lm/flush! log-mgr lsn2)
      (is (= ["Record 1" "Record 2"] (read-records log-mgr))))))

(deftest test-multiple-flushes
  (testing "Multiple flushes preserve all data"
    (let [[log-mgr] (make-test-log-mgr)]
      (lm/append! log-mgr (str->bytes "First"))
      (lm/flush! log-mgr)
      (lm/append! log-mgr (str->bytes "Second"))
      (lm/flush! log-mgr)
      (lm/append! log-mgr (str->bytes "Third"))
      (lm/flush! log-mgr)
      (is (= ["First" "Second" "Third"] (read-records log-mgr))))))

(deftest test-large-record
  (testing "Appending a record near block size boundary"
    (let [[log-mgr] (make-test-log-mgr)
          large-record (apply str (repeat (- block-size 20) "A"))]
      (lm/append! log-mgr (str->bytes large-record))
      (lm/flush! log-mgr)
      (is (= [large-record] (read-records log-mgr))))))

(deftest test-block-boundary
  (testing "Records spanning multiple blocks are handled correctly"
    (let [[log-mgr] (make-test-log-mgr)
          large-record (apply str (repeat 100 "X"))
          record-count 5
          lsns (repeatedly record-count #(lm/append! log-mgr (str->bytes large-record)))]
      (is (= (range 1 (inc record-count)) lsns))
      (lm/flush! log-mgr)
      (let [records (read-records log-mgr)]
        (is (= record-count (count records)))
        (is (every? #(= large-record %) records))))))

(deftest test-persistence
  (testing "Log persists across log manager instances"
    (let [log-file (unique-log-file)
          file-mgr (make-file-mgr test-dir block-size)
          records ["First" "Second" "Third"]]
      (let [log-mgr1 (lm/make-log-mgr file-mgr log-file)]
        (append-records log-mgr1 records)
        (lm/flush! log-mgr1))
      (let [file-mgr2 (make-file-mgr test-dir block-size)
            log-mgr2 (lm/make-log-mgr file-mgr2 log-file)]
        (is (= records (read-records log-mgr2)))))))

(deftest test-binary-data
  (testing "Appending and retrieving binary data"
    (let [[log-mgr] (make-test-log-mgr)
          binary-data (byte-array (range -128 128))]
      (lm/append! log-mgr binary-data)
      (lm/flush! log-mgr)
      (is (= (seq binary-data) (seq (first (seq log-mgr))))))))

(deftest test-concurrent-appends
  (testing "Thread-safe appending from multiple threads"
    (let [[log-mgr] (make-test-log-mgr)
          thread-count 10
          records-per-thread 10
          expected-total (* thread-count records-per-thread)
          futures (doall
                   (for [i (range thread-count)]
                     (future
                       (dotimes [j records-per-thread]
                         (lm/append! log-mgr (str->bytes (str "Thread-" i "-Record-" j)))))))]
      (doseq [f futures] @f)
      (lm/flush! log-mgr)
      (is (= expected-total (count (seq log-mgr)))))))
