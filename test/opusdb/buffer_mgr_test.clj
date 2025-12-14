(ns opusdb.buffer-mgr-test
  (:require [clojure.test :refer [deftest is use-fixtures testing]]
            [opusdb.buffer :as b]
            [opusdb.buffer-mgr :as bm]
            [opusdb.file :as fm]
            [opusdb.log :as lm])
  (:import [java.io File]))

(defn setup [pool-size]
  (let [file-bm (fm/make-file-mgr "test-db" 400)
        log-bm (lm/make-log-mgr file-bm "test-log")]
    {:file-bm file-bm
     :log-bm log-bm
     :buffer-bm (bm/make-buffer-mgr file-bm log-bm pool-size)}))

(defn ensure-block-exists [file-bm block-id]
  (when-not (.exists (File. (str (:db-dir file-bm) "/" (:file-name block-id))))
    (fm/append file-bm (:file-name block-id))))

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

(deftest buffer-bm-initial-state
  (testing "BufferBm initial state"
    (let [{:keys [buffer-bm]} (setup 3)]
      (is (= 3 (bm/available buffer-bm))
          "All buffers should be available initially"))))

(deftest buffer-bm-available-count
  (testing "BufferBm available count tracking"
    (let [{:keys [file-bm buffer-bm]} (setup 3)
          block-id {:file-name "f1" :index 0}]
      (ensure-block-exists file-bm block-id)

      (is (= 3 (bm/available buffer-bm)) "Initial available count")

      (let [buffer1 (bm/pin-buffer buffer-bm block-id)]
        (is (= 2 (bm/available buffer-bm))
            "Available count decreases after pinning")

        (let [buf2 (bm/pin-buffer buffer-bm block-id)]
          (is (= buffer1 buf2) "Same buffer returned for same block")
          (is (= 2 (bm/available buffer-bm))
              "Available count unchanged when pinning same buffer"))

        (bm/unpin-buffer buffer-bm buffer1)
        (is (= 2 (bm/available buffer-bm))
            "Available count unchanged after first unpin (still pinned)")

        (bm/unpin-buffer buffer-bm buffer1)
        (is (= 3 (bm/available buffer-bm))
            "Available count increases when buffer fully unpinned")))))

(deftest buffer-bm-multiple-blocks
  (testing "BufferBm handling multiple blocks"
    (let [{:keys [file-bm buffer-bm]} (setup 3)
          block-id1 {:file-name "f1" :index 0}
          block-id2 {:file-name "f1" :index 1}
          block-id3 {:file-name "f2" :index 0}]
      (ensure-block-exists file-bm block-id1)
      (ensure-block-exists file-bm block-id2)
      (ensure-block-exists file-bm block-id3)

      (let [buffer1 (bm/pin-buffer buffer-bm block-id1)
            buf2 (bm/pin-buffer buffer-bm block-id2)
            buf3 (bm/pin-buffer buffer-bm block-id3)]
        (is (not= buffer1 buf2) "Different buffers for different blocks")
        (is (not= buf2 buf3) "Different buffers for different blocks")
        (is (not= buffer1 buf3) "Different buffers for different blocks")

        (is (= 0 (bm/available buffer-bm))
            "No buffers available when all are pinned")

        (is (= block-id1 (b/block-id buffer1)) "Buffer 1 has correct block")
        (is (= block-id2 (b/block-id buf2)) "Buffer 2 has correct block")
        (is (= block-id3 (b/block-id buf3)) "Buffer 3 has correct block")))))

(deftest buffer-bm-reuse
  (testing "BufferBm reuses unpinned buffers"
    (let [{:keys [file-bm buffer-bm]} (setup 2)
          block-id1 {:file-name "f1" :index 0}
          block-id2 {:file-name "f1" :index 1}
          block-id3 {:file-name "f1" :index 2}]
      (ensure-block-exists file-bm block-id1)
      (ensure-block-exists file-bm block-id2)
      (ensure-block-exists file-bm block-id3)

      (let [buffer1 (bm/pin-buffer buffer-bm block-id1)]
        (bm/pin-buffer buffer-bm block-id2)
        (is (= 0 (bm/available buffer-bm)))

        (bm/unpin-buffer buffer-bm buffer1)
        (is (= 1 (bm/available buffer-bm)))

        (let [buf3 (bm/pin-buffer buffer-bm block-id3)]
          (is (= buffer1 buf3) "Unpinned buffer should be reused")
          (is (= block-id3 (b/block-id buf3)) "Reused buffer has new block")
          (is (= 0 (bm/available buffer-bm))))))))

(deftest buffer-bm-timeout
  (testing "BufferBm timeout when no buffers available"
    (let [{:keys [file-bm buffer-bm]} (setup 1)
          block-id1 {:file-name "f1" :index 0}
          block-id2 {:file-name "f1" :index 1}]
      (ensure-block-exists file-bm block-id1)
      (ensure-block-exists file-bm block-id2)

      (bm/pin-buffer buffer-bm block-id1)
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Buffer abort: waiting too long"
                            (bm/pin-buffer buffer-bm block-id2))
          "Should timeout when no buffers available"))))

(deftest buffer-bm-flush-all
  (testing "BufferBm flush-all for specific transaction"
    (let [{:keys [file-bm buffer-bm]} (setup 3)
          block-id1 {:file-name "f1" :index 0}
          block-id2 {:file-name "f1" :index 1}
          block-id3 {:file-name "f1" :index 2}]
      (ensure-block-exists file-bm block-id1)
      (ensure-block-exists file-bm block-id2)
      (ensure-block-exists file-bm block-id3)

      (let [buffer1 (bm/pin-buffer buffer-bm block-id1)
            buffer2 (bm/pin-buffer buffer-bm block-id2)
            buf3 (bm/pin-buffer buffer-bm block-id3)]
        (b/mark-dirty buffer1 10 100)
        (b/mark-dirty buffer2 10 101)
        (b/mark-dirty buf3 20 102)

        (bm/flush-all buffer-bm 10)

        (is (= -1 (b/txid buffer1)) "Buffer 1 should be flushed")
        (is (= -1 (b/txid buffer2)) "Buffer 2 should be flushed")
        (is (= 20 (b/txid buf3)) "Buffer 3 should not be flushed")))))

(deftest buffer-bm-flush-all-selective
  (testing "BufferBm flush-all only affects specified transaction"
    (let [{:keys [file-bm buffer-bm]} (setup 4)
          blocks (map #(hash-map :file-name "f1" :index %) (range 4))]
      (doseq [block-id blocks]
        (ensure-block-exists file-bm block-id))

      (let [buffers (mapv #(bm/pin-buffer buffer-bm %) blocks)]
        (b/mark-dirty (nth buffers 0) 100 1000)
        (b/mark-dirty (nth buffers 1) 100 1001)
        (b/mark-dirty (nth buffers 2) 200 1002)
        (b/mark-dirty (nth buffers 3) 300 1003)

        (bm/flush-all buffer-bm 100)

        (is (= -1 (b/txid (nth buffers 0))))
        (is (= -1 (b/txid (nth buffers 1))))
        (is (= 200 (b/txid (nth buffers 2))))
        (is (= 300 (b/txid (nth buffers 3))))))))

(deftest buffer-bm-concurrent
  (testing "BufferBm concurrent access"
    (let [{:keys [file-bm buffer-bm]} (setup 3)
          block-id {:file-name "f1" :index 0}
          results (atom [])]
      (ensure-block-exists file-bm block-id)

      (let [threads (repeatedly 5
                                #(Thread.
                                  (fn []
                                    (try
                                      (let [buf (bm/pin-buffer buffer-bm block-id)]
                                        (Thread/sleep 10)
                                        (bm/unpin-buffer buffer-bm buf)
                                        (swap! results conj :success))
                                      (catch Exception _
                                        (swap! results conj :error))))))]
        (doseq [t threads] (.start t))
        (doseq [t threads] (.join t))

        (is (= 5 (count @results)) "All threads completed")
        (is (every? #(= :success %) @results) "All threads succeeded")
        (is (= 3 (bm/available buffer-bm))
            "All buffers available after concurrent access")))))

(deftest buffer-bm-pin-same-block-multiple-times
  (testing "Pinning same block multiple times returns same buffer"
    (let [{:keys [file-bm buffer-bm]} (setup 3)
          block-id {:file-name "f1" :index 0}]
      (ensure-block-exists file-bm block-id)

      (let [buffer1 (bm/pin-buffer buffer-bm block-id)
            buffer2 (bm/pin-buffer buffer-bm block-id)
            buffer3 (bm/pin-buffer buffer-bm block-id)]
        (is (= buffer1 buffer2 buffer3) "All returns should be same buffer")
        (is (= 3 (b/pin-count buffer1)) "Pin count should be 3")
        (is (= 2 (bm/available buffer-bm))
            "Only one buffer unavailable")))))

(deftest buffer-bm-unpin-decrements-correctly
  (testing "Unpinning decrements pin count correctly"
    (let [{:keys [file-bm buffer-bm]} (setup 2)
          block-id {:file-name "f1" :index 0}]
      (ensure-block-exists file-bm block-id)

      (let [buffer (bm/pin-buffer buffer-bm block-id)]
        (bm/pin-buffer buffer-bm block-id)
        (bm/pin-buffer buffer-bm block-id)
        (is (= 3 (b/pin-count buffer)))

        (bm/unpin-buffer buffer-bm buffer)
        (is (= 2 (b/pin-count buffer)))
        (is (= 1 (bm/available buffer-bm)) "Still pinned")

        (bm/unpin-buffer buffer-bm buffer)
        (is (= 1 (b/pin-count buffer)))
        (is (= 1 (bm/available buffer-bm)) "Still pinned")

        (bm/unpin-buffer buffer-bm buffer)
        (is (= 0 (b/pin-count buffer)))
        (is (= 2 (bm/available buffer-bm)) "Now unpinned")))))
