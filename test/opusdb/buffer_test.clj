(ns opusdb.buffer-test
  (:require [clojure.test :refer [deftest is use-fixtures testing]]
            [opusdb.buffer :as buffer]
            [opusdb.file :as file]
            [opusdb.log :as log])
  (:import [java.io File]))

(defn setup []
  (let [file-mgr (file/make-file-mgr "test-db" 400)
        log-mgr (log/make-log-mgr file-mgr "test-log")]
    {:file-mgr file-mgr
     :log-mgr log-mgr}))

(defn ensure-block-exists [file-mgr block]
  (when-not (.exists (File. (str (:db-dir file-mgr) "/" (:file-name block))))
    (.append file-mgr (:file-name block))))

(defn cleanup-fixture [f]
  (try
    (f)
    (finally
      (let [db-dir (File. "test-db")]
        (when (.exists db-dir)
          (doseq [file (.listFiles db-dir)]
            (.delete file))
          (.delete db-dir))))))

(use-fixtures :each cleanup-fixture)

(deftest buffer-initial-state
  (testing "Buffer should start with proper initial state"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (buffer/make-buffer file-mgr log-mgr)]
      (is (= -1 (buffer/txid buf)) "Initial txid should be -1")
      (is (= -1 (buffer/lsn buf)) "Initial lsn should be -1")
      (is (nil? (buffer/block buf)) "Initial block should be nil")
      (is (false? (buffer/pinned? buf)) "Buffer should not be pinned initially")
      (is (= 0 (buffer/pin-count buf)) "Pin count should be 0"))))

(deftest buffer-pinning
  (testing "Buffer pin and unpin operations"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (buffer/make-buffer file-mgr log-mgr)]
      (is (false? (buffer/pinned? buf)) "Buffer starts unpinned")
      (is (= 0 (buffer/pin-count buf)))

      (buffer/pin buf)
      (is (true? (buffer/pinned? buf)) "Buffer is pinned after pin")
      (is (= 1 (buffer/pin-count buf)))

      (buffer/pin buf)
      (is (true? (buffer/pinned? buf)) "Buffer is still pinned after second pin")
      (is (= 2 (buffer/pin-count buf)))

      (buffer/unpin buf)
      (is (true? (buffer/pinned? buf)) "Buffer is still pinned after one unpin")
      (is (= 1 (buffer/pin-count buf)))

      (buffer/unpin buf)
      (is (false? (buffer/pinned? buf)) "Buffer is unpinned after matching unpins")
      (is (= 0 (buffer/pin-count buf))))))

(deftest buffer-mark-dirty
  (testing "Buffer mark dirty with valid transaction ID"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (buffer/make-buffer file-mgr log-mgr)]
      (is (= -1 (buffer/txid buf)))

      (buffer/mark-dirty buf 42 100)
      (is (= 42 (buffer/txid buf)) "Transaction ID should be updated")
      (is (= 100 (buffer/lsn buf)) "LSN should be updated")

      (buffer/mark-dirty buf 10 200)
      (is (= 10 (buffer/txid buf)) "Transaction ID should be updated again")
      (is (= 200 (buffer/lsn buf)) "LSN should be updated again")))

  (testing "Buffer mark dirty with negative LSN preserves old LSN"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (buffer/make-buffer file-mgr log-mgr)]
      (buffer/mark-dirty buf 42 100)
      (buffer/mark-dirty buf 10 -1)
      (is (= 10 (buffer/txid buf)) "Transaction ID should be updated")
      (is (= 100 (buffer/lsn buf)) "LSN should not change when negative")))

  (testing "Buffer mark dirty with negative transaction ID throws exception"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (buffer/make-buffer file-mgr log-mgr)]
      (is (thrown? IllegalArgumentException
                   (buffer/mark-dirty buf -5 200))
          "Negative transaction ID should throw exception"))))

(deftest buffer-flush-without-block
  (testing "Flushing buffer without assigned block throws exception"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (buffer/make-buffer file-mgr log-mgr)]
      (buffer/mark-dirty buf 42 100)
      (is (thrown-with-msg? IllegalStateException
                            #"Cannot flush: buffer has no assigned block"
                            (buffer/flush buf))))))

(deftest buffer-flush-clean-buffer
  (testing "Flushing clean buffer (txid=-1) does nothing"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (buffer/make-buffer file-mgr log-mgr)
          blk {:file-name "f1" :block-id 0}]
      (ensure-block-exists file-mgr blk)
      (buffer/assign-to-block buf blk)
      (is (= -1 (buffer/txid buf)))
      (buffer/flush buf) ; Should not throw
      (is (= -1 (buffer/txid buf))))))

(deftest buffer-assign-and-flush
  (testing "Buffer assignment and flushing"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (buffer/make-buffer file-mgr log-mgr)
          blk1 {:file-name "f1" :block-id 0}
          blk2 {:file-name "f1" :block-id 1}]
      (ensure-block-exists file-mgr blk1)
      (ensure-block-exists file-mgr blk2)

      (buffer/assign-to-block buf blk1)
      (is (= blk1 (buffer/block buf)) "Block should be assigned")
      (is (false? (buffer/pinned? buf)) "Buffer should not be pinned after assignment")
      (is (= -1 (buffer/txid buf)) "Transaction ID should be -1 after assignment")

      (buffer/mark-dirty buf 42 100)
      (is (= 42 (buffer/txid buf)) "Buffer should be dirty")

      (buffer/assign-to-block buf blk2)
      (is (= blk2 (buffer/block buf)) "Block should be reassigned")
      (is (= -1 (buffer/txid buf)) "Transaction ID should be reset after flush during assignment"))))

(deftest buffer-explicit-flush
  (testing "Explicit buffer flush resets transaction ID"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (buffer/make-buffer file-mgr log-mgr)
          blk {:file-name "f1" :block-id 0}]
      (ensure-block-exists file-mgr blk)
      (buffer/assign-to-block buf blk)
      (buffer/mark-dirty buf 42 100)
      (is (= 42 (buffer/txid buf)))

      (buffer/flush buf)
      (is (= -1 (buffer/txid buf)) "Transaction ID should be reset after flush")
      (is (= blk (buffer/block buf)) "Block should remain assigned"))))
