(ns opusdb.buffer-mgr-test
  (:require [clojure.test :refer [deftest is use-fixtures testing]]
            [opusdb.buffer :as buffer]
            [opusdb.buffer-mgr :as mgr]
            [opusdb.file :as file]
            [opusdb.log :as log])
  (:import [java.io File]))

(defn setup [pool-size]
  (let [file-mgr (file/make-file-mgr "test-db" 400)
        log-mgr (log/make-log-mgr file-mgr "test-log")]
    {:file-mgr file-mgr
     :log-mgr log-mgr
     :buffer-mgr (mgr/make-buffer-mgr file-mgr log-mgr pool-size)}))

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

(deftest buffer-mgr-initial-state
  (testing "BufferMgr initial state"
    (let [{:keys [buffer-mgr]} (setup 3)]
      (is (= 3 (mgr/available buffer-mgr))
          "All buffers should be available initially"))))

(deftest buffer-mgr-available-count
  (testing "BufferMgr available count tracking"
    (let [{:keys [file-mgr buffer-mgr]} (setup 3)
          blk {:file-name "f1" :block-id 0}]
      (ensure-block-exists file-mgr blk)

      (is (= 3 (mgr/available buffer-mgr)) "Initial available count")

      (let [buf1 (mgr/pin-buffer buffer-mgr blk)]
        (is (= 2 (mgr/available buffer-mgr))
            "Available count decreases after pinning")

        (let [buf2 (mgr/pin-buffer buffer-mgr blk)]
          (is (= buf1 buf2) "Same buffer returned for same block")
          (is (= 2 (mgr/available buffer-mgr))
              "Available count unchanged when pinning same buffer"))

        (mgr/unpin-buffer buffer-mgr buf1)
        (is (= 2 (mgr/available buffer-mgr))
            "Available count unchanged after first unpin (still pinned)")

        (mgr/unpin-buffer buffer-mgr buf1)
        (is (= 3 (mgr/available buffer-mgr))
            "Available count increases when buffer fully unpinned")))))

(deftest buffer-mgr-multiple-blocks
  (testing "BufferMgr handling multiple blocks"
    (let [{:keys [file-mgr buffer-mgr]} (setup 3)
          blk1 {:file-name "f1" :block-id 0}
          blk2 {:file-name "f1" :block-id 1}
          blk3 {:file-name "f2" :block-id 0}]
      (ensure-block-exists file-mgr blk1)
      (ensure-block-exists file-mgr blk2)
      (ensure-block-exists file-mgr blk3)

      (let [buf1 (mgr/pin-buffer buffer-mgr blk1)
            buf2 (mgr/pin-buffer buffer-mgr blk2)
            buf3 (mgr/pin-buffer buffer-mgr blk3)]
        (is (not= buf1 buf2) "Different buffers for different blocks")
        (is (not= buf2 buf3) "Different buffers for different blocks")
        (is (not= buf1 buf3) "Different buffers for different blocks")

        (is (= 0 (mgr/available buffer-mgr))
            "No buffers available when all are pinned")

        (is (= blk1 (buffer/block buf1)) "Buffer 1 has correct block")
        (is (= blk2 (buffer/block buf2)) "Buffer 2 has correct block")
        (is (= blk3 (buffer/block buf3)) "Buffer 3 has correct block")))))

(deftest buffer-mgr-reuse
  (testing "BufferMgr reuses unpinned buffers"
    (let [{:keys [file-mgr buffer-mgr]} (setup 2)
          blk1 {:file-name "f1" :block-id 0}
          blk2 {:file-name "f1" :block-id 1}
          blk3 {:file-name "f1" :block-id 2}]
      (ensure-block-exists file-mgr blk1)
      (ensure-block-exists file-mgr blk2)
      (ensure-block-exists file-mgr blk3)

      (let [buf1 (mgr/pin-buffer buffer-mgr blk1)]
        (mgr/pin-buffer buffer-mgr blk2)
        (is (= 0 (mgr/available buffer-mgr)))

        (mgr/unpin-buffer buffer-mgr buf1)
        (is (= 1 (mgr/available buffer-mgr)))

        (let [buf3 (mgr/pin-buffer buffer-mgr blk3)]
          (is (= buf1 buf3) "Unpinned buffer should be reused")
          (is (= blk3 (buffer/block buf3)) "Reused buffer has new block")
          (is (= 0 (mgr/available buffer-mgr))))))))

(deftest buffer-mgr-timeout
  (testing "BufferMgr timeout when no buffers available"
    (let [{:keys [file-mgr buffer-mgr]} (setup 1)
          blk1 {:file-name "f1" :block-id 0}
          blk2 {:file-name "f1" :block-id 1}]
      (ensure-block-exists file-mgr blk1)
      (ensure-block-exists file-mgr blk2)

      (mgr/pin-buffer buffer-mgr blk1)
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Buffer abort: waiting too long"
                            (mgr/pin-buffer buffer-mgr blk2))
          "Should timeout when no buffers available"))))

(deftest buffer-mgr-flush-all
  (testing "BufferMgr flush-all for specific transaction"
    (let [{:keys [file-mgr buffer-mgr]} (setup 3)
          blk1 {:file-name "f1" :block-id 0}
          blk2 {:file-name "f1" :block-id 1}
          blk3 {:file-name "f1" :block-id 2}]
      (ensure-block-exists file-mgr blk1)
      (ensure-block-exists file-mgr blk2)
      (ensure-block-exists file-mgr blk3)

      (let [buf1 (mgr/pin-buffer buffer-mgr blk1)
            buf2 (mgr/pin-buffer buffer-mgr blk2)
            buf3 (mgr/pin-buffer buffer-mgr blk3)]
        (buffer/mark-dirty buf1 10 100)
        (buffer/mark-dirty buf2 10 101)
        (buffer/mark-dirty buf3 20 102)

        (mgr/flush-all buffer-mgr 10)

        (is (= -1 (buffer/txid buf1)) "Buffer 1 should be flushed")
        (is (= -1 (buffer/txid buf2)) "Buffer 2 should be flushed")
        (is (= 20 (buffer/txid buf3)) "Buffer 3 should not be flushed")))))

(deftest buffer-mgr-flush-all-selective
  (testing "BufferMgr flush-all only affects specified transaction"
    (let [{:keys [file-mgr buffer-mgr]} (setup 4)
          blocks (map #(hash-map :file-name "f1" :block-id %) (range 4))]
      (doseq [blk blocks]
        (ensure-block-exists file-mgr blk))

      (let [buffers (mapv #(mgr/pin-buffer buffer-mgr %) blocks)]
        (buffer/mark-dirty (nth buffers 0) 100 1000)
        (buffer/mark-dirty (nth buffers 1) 100 1001)
        (buffer/mark-dirty (nth buffers 2) 200 1002)
        (buffer/mark-dirty (nth buffers 3) 300 1003)

        (mgr/flush-all buffer-mgr 100)

        (is (= -1 (buffer/txid (nth buffers 0))))
        (is (= -1 (buffer/txid (nth buffers 1))))
        (is (= 200 (buffer/txid (nth buffers 2))))
        (is (= 300 (buffer/txid (nth buffers 3))))))))

(deftest buffer-mgr-concurrent
  (testing "BufferMgr concurrent access"
    (let [{:keys [file-mgr buffer-mgr]} (setup 3)
          blk {:file-name "f1" :block-id 0}
          results (atom [])]
      (ensure-block-exists file-mgr blk)

      (let [threads (repeatedly 5
                                #(Thread.
                                  (fn []
                                    (try
                                      (let [buf (mgr/pin-buffer buffer-mgr blk)]
                                        (Thread/sleep 10)
                                        (mgr/unpin-buffer buffer-mgr buf)
                                        (swap! results conj :success))
                                      (catch Exception _
                                        (swap! results conj :error))))))]
        (doseq [t threads] (.start t))
        (doseq [t threads] (.join t))

        (is (= 5 (count @results)) "All threads completed")
        (is (every? #(= :success %) @results) "All threads succeeded")
        (is (= 3 (mgr/available buffer-mgr))
            "All buffers available after concurrent access")))))

(deftest buffer-mgr-pin-same-block-multiple-times
  (testing "Pinning same block multiple times returns same buffer"
    (let [{:keys [file-mgr buffer-mgr]} (setup 3)
          blk {:file-name "f1" :block-id 0}]
      (ensure-block-exists file-mgr blk)

      (let [buf1 (mgr/pin-buffer buffer-mgr blk)
            buf2 (mgr/pin-buffer buffer-mgr blk)
            buf3 (mgr/pin-buffer buffer-mgr blk)]
        (is (= buf1 buf2 buf3) "All returns should be same buffer")
        (is (= 3 (buffer/pin-count buf1)) "Pin count should be 3")
        (is (= 2 (mgr/available buffer-mgr))
            "Only one buffer unavailable")))))

(deftest buffer-mgr-unpin-decrements-correctly
  (testing "Unpinning decrements pin count correctly"
    (let [{:keys [file-mgr buffer-mgr]} (setup 2)
          blk {:file-name "f1" :block-id 0}]
      (ensure-block-exists file-mgr blk)

      (let [buf (mgr/pin-buffer buffer-mgr blk)]
        (mgr/pin-buffer buffer-mgr blk)
        (mgr/pin-buffer buffer-mgr blk)
        (is (= 3 (buffer/pin-count buf)))

        (mgr/unpin-buffer buffer-mgr buf)
        (is (= 2 (buffer/pin-count buf)))
        (is (= 1 (mgr/available buffer-mgr)) "Still pinned")

        (mgr/unpin-buffer buffer-mgr buf)
        (is (= 1 (buffer/pin-count buf)))
        (is (= 1 (mgr/available buffer-mgr)) "Still pinned")

        (mgr/unpin-buffer buffer-mgr buf)
        (is (= 0 (buffer/pin-count buf)))
        (is (= 2 (mgr/available buffer-mgr)) "Now unpinned")))))
