(ns opusdb.buffer-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [opusdb.buffer :as b]
   [opusdb.file-mgr :as fm]
   [opusdb.log-mgr :as lm])
  (:import
   [java.io File]))

(defn setup []
  (let [file-mgr (fm/make-file-mgr "test-db" 400)
        log-mgr (lm/make-log-mgr file-mgr "test-log")]
    {:file-mgr file-mgr
     :log-mgr log-mgr}))

(defn ensure-block-exists [file-mgr block]
  (when-not (.exists (File. (str (:db-dir file-mgr) "/" (:file-name block))))
    (fm/append! file-mgr (:file-name block))))

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
          buf (b/make-buffer file-mgr log-mgr)
          state @(:state buf)]
      (is (= -1 (:tx-id state)) "Initial tx-id should be -1")
      (is (= -1 (:lsn state)) "Initial lsn should be -1")
      (is (nil? (:block-id state)) "Initial block should be nil")
      (is (false? (b/pinned? buf)) "Buffer should not be pinned initially")
      (is (= 0 (:pin-count @(:state buf))) "Pin count should be 0"))))

(deftest buffer-pinning
  (testing "Buffer pin and unpin operations"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buffer (b/make-buffer file-mgr log-mgr)]
      (is (false? (b/pinned? buffer)) "Buffer starts unpinned")
      (is (= 0 (:pin-count @(:state buffer))))

      (b/pin! buffer)
      (is (true? (b/pinned? buffer)) "Buffer is pinned after pin")
      (is (= 1 (:pin-count @(:state buffer))))

      (b/pin! buffer)
      (is (true? (b/pinned? buffer)) "Buffer is still pinned after second pin")
      (is (= 2 (:pin-count @(:state buffer))))

      (b/unpin! buffer)
      (is (true? (b/pinned? buffer)) "Buffer is still pinned after one unpin")
      (is (= 1 (:pin-count @(:state buffer))))

      (b/unpin! buffer)
      (is (false? (b/pinned? buffer)) "Buffer is unpinned after matching unpins")
      (is (= 0 (:pin-count @(:state buffer)))))))

(deftest buffer-mark-dirty
  (testing "Buffer mark dirty with valid transaction ID"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buffer (b/make-buffer file-mgr log-mgr)
          state (:state buffer)]
      (is (= -1 (:tx-id @state)))

      (b/smear! buffer 42 100)
      (is (= 42 (:tx-id @state)) "Transaction ID should be updated")
      (is (= 100 (:lsn @state)) "LSN should be updated")

      (b/smear! buffer 10 200)
      (is (= 10 (:tx-id @state)) "Transaction ID should be updated again")
      (is (= 200 (:lsn @state)) "LSN should be updated again")))

  (testing "Buffer mark dirty with negative LSN preserves old LSN"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (b/make-buffer file-mgr log-mgr)
          state (:state buf)]
      (b/smear! buf 42 100)
      (b/smear! buf 10 -1)
      (is (= 10 (:tx-id @state)) "Transaction ID should be updated")
      (is (= 100 (:lsn @state)) "LSN should not change when negative")))

  (testing "Buffer mark dirty with negative transaction ID throws exception"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buffer (b/make-buffer file-mgr log-mgr)]
      (is (thrown? IllegalArgumentException
                   (b/smear! buffer -5 200))
          "Negative transaction ID should throw exception"))))

(deftest buffer-flush-without-block
  (testing "Flushing buffer without assigned block throws exception"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buffer (b/make-buffer file-mgr log-mgr)]
      (b/smear! buffer 42 100)
      (is (thrown-with-msg? IllegalStateException
                            #"Cannot flush: buffer has no assigned block"
                            (b/flush! buffer))))))

(deftest buffer-flush-clean-buffer
  (testing "Flushing clean buffer (tx-id=-1) does nothing"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (b/make-buffer file-mgr log-mgr)
          state (:state buf)
          block-id {:file-name "f1" :index 0}]
      (ensure-block-exists file-mgr block-id)
      (b/assign-to-block! buf block-id)
      (is (= -1 (:tx-id @state)))
      (b/flush! buf) ; Should not throw
      (is (= -1 (:tx-id @state))))))

(deftest buffer-assign-and-flush
  (testing "Buffer assignment and flushing"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (b/make-buffer file-mgr log-mgr)
          state (:state buf)
          block-id1 {:file-name "f1" :index 0}
          block-id2 {:file-name "f1" :index 1}]
      (ensure-block-exists file-mgr block-id1)
      (ensure-block-exists file-mgr block-id2)

      (b/assign-to-block! buf block-id1)
      (is (= block-id1 (:block-id @state)) "Block should be assigned")
      (is (false? (b/pinned? buf)) "Buffer should not be pinned after assignment")
      (is (= -1 (:tx-id @state)) "Transaction ID should be -1 after assignment")

      (b/smear! buf 42 100)
      (is (= 42 (:tx-id @state)) "Buffer should be dirty")

      (b/assign-to-block! buf block-id2)
      (is (= block-id2 (:block-id @state)) "Block should be reassigned")
      (is (= -1 (:tx-id @state)) "Transaction ID should be reset after flush during assignment"))))

(deftest buffer-explicit-flush
  (testing "Explicit buffer flush resets transaction ID"
    (let [{:keys [file-mgr log-mgr]} (setup)
          buf (b/make-buffer file-mgr log-mgr)
          state (:state buf)
          block-id {:file-name "f1" :index 0}]
      (ensure-block-exists file-mgr block-id)
      (b/assign-to-block! buf block-id)
      (b/smear! buf 42 100)
      (is (= 42 (:tx-id @state)))

      (b/flush! buf)
      (is (= -1 (:tx-id @state)) "Transaction ID should be reset after flush")
      (is (= block-id (:block-id @state)) "Block should remain assigned"))))
