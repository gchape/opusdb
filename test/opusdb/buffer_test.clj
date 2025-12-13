(ns opusdb.buffer-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [opusdb.buffer :as buffer]
            [opusdb.file :as file]
            [opusdb.log :as log])
  (:import [java.io File]))

(defn setup [pool-size]
  (let [file-mgr (file/make-file-mgr "test-db" 400)
        log-mgr (log/make-log-mgr file-mgr "test-log")]
    {:file-mgr file-mgr
     :log-mgr log-mgr
     :buffer-mgr (buffer/make-buffer-mgr file-mgr log-mgr pool-size)}))

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

;; Buffer Tests
(deftest buffer-initial-state
  (let [{:keys [file-mgr log-mgr]} (setup 3)
        buf (buffer/make-buffer file-mgr log-mgr)]
    (is (= -1 (.txId buf)))
    (is (nil? (.block buf)))
    (is (false? (.isPinned buf)))))

(deftest buffer-pinning
  (let [{:keys [file-mgr log-mgr]} (setup 3)
        buf (buffer/make-buffer file-mgr log-mgr)]
    (is (false? (.isPinned buf)))
    (.pin buf)
    (is (true? (.isPinned buf)))
    (.pin buf)
    (is (true? (.isPinned buf)))
    (.unpin buf)
    (is (true? (.isPinned buf)))
    (.unpin buf)
    (is (false? (.isPinned buf)))))

(deftest buffer-mark-dirty
  (let [{:keys [file-mgr log-mgr]} (setup 3)
        buf (buffer/make-buffer file-mgr log-mgr)]
    (is (= -1 (.txId buf)))
    (.markDirty buf 42 100)
    (is (= 42 (.txId buf)))
    (is (thrown? IllegalArgumentException (.markDirty buf -5 200)))
    (.markDirty buf 10 -1)
    (is (= 10 (.txId buf)))))

(deftest buffer-flush-without-block
  (let [{:keys [file-mgr log-mgr]} (setup 3)
        buf (buffer/make-buffer file-mgr log-mgr)]
    (.markDirty buf 42 100)
    (is (thrown-with-msg? IllegalStateException
                          #"Cannot flush: buffer has no assigned block"
                          (.flush buf)))))

(deftest buffer-assign-and-flush
  (let [{:keys [file-mgr log-mgr]} (setup 3)
        buf (buffer/make-buffer file-mgr log-mgr)
        blk1 {:file-name "f1" :block-id 0}
        blk2 {:file-name "f1" :block-id 1}]
    (ensure-block-exists file-mgr blk1)
    (ensure-block-exists file-mgr blk2)
    (.assignToBlock buf blk1)
    (is (= blk1 (.block buf)))
    (is (false? (.isPinned buf)))
    (.markDirty buf 42 100)
    (.assignToBlock buf blk2)
    (is (= blk2 (.block buf)))
    (is (= -1 (.txId buf)))))

;; Buffer Manager Tests
(deftest buffer-mgr-available-count
  (let [{:keys [file-mgr buffer-mgr]} (setup 3)
        blk {:file-name "f1" :block-id 0}]
    (ensure-block-exists file-mgr blk)
    (is (= 3 (.available buffer-mgr)))
    (let [buf1 (.pin buffer-mgr blk)]
      (is (= 2 (.available buffer-mgr)))
      (let [buf2 (.pin buffer-mgr blk)]
        (is (= buf1 buf2))
        (is (= 2 (.available buffer-mgr))))
      (.unpin buffer-mgr buf1)
      (is (= 2 (.available buffer-mgr)))
      (.unpin buffer-mgr buf1)
      (is (= 3 (.available buffer-mgr))))))

(deftest buffer-mgr-multiple-blocks
  (let [{:keys [file-mgr buffer-mgr]} (setup 3)
        blk1 {:file-name "f1" :block-id 0}
        blk2 {:file-name "f1" :block-id 1}
        blk3 {:file-name "f2" :block-id 0}]
    (ensure-block-exists file-mgr blk1)
    (ensure-block-exists file-mgr blk2)
    (ensure-block-exists file-mgr blk3)
    (let [buf1 (.pin buffer-mgr blk1)
          buf2 (.pin buffer-mgr blk2)
          buf3 (.pin buffer-mgr blk3)]
      (is (not= buf1 buf2))
      (is (not= buf2 buf3))
      (is (not= buf1 buf3))
      (is (= 0 (.available buffer-mgr)))
      (is (= blk1 (.block buf1)))
      (is (= blk2 (.block buf2)))
      (is (= blk3 (.block buf3))))))

(deftest buffer-mgr-reuse
  (let [{:keys [file-mgr buffer-mgr]} (setup 2)
        blk1 {:file-name "f1" :block-id 0}
        blk2 {:file-name "f1" :block-id 1}
        blk3 {:file-name "f1" :block-id 2}]
    (ensure-block-exists file-mgr blk1)
    (ensure-block-exists file-mgr blk2)
    (ensure-block-exists file-mgr blk3)
    (let [buf1 (.pin buffer-mgr blk1)]
      (.pin buffer-mgr blk2)
      (is (= 0 (.available buffer-mgr)))
      (.unpin buffer-mgr buf1)
      (is (= 1 (.available buffer-mgr)))
      (let [buf3 (.pin buffer-mgr blk3)]
        (is (= buf1 buf3))
        (is (= blk3 (.block buf3)))
        (is (= 0 (.available buffer-mgr)))))))

(deftest buffer-mgr-timeout
  (let [{:keys [file-mgr buffer-mgr]} (setup 1)
        blk1 {:file-name "f1" :block-id 0}
        blk2 {:file-name "f1" :block-id 1}]
    (ensure-block-exists file-mgr blk1)
    (ensure-block-exists file-mgr blk2)
    (.pin buffer-mgr blk1)
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Buffer abort: waiting too long"
                          (.pin buffer-mgr blk2)))))

(deftest buffer-mgr-flush-all
  (let [{:keys [file-mgr buffer-mgr]} (setup 3)
        blk1 {:file-name "f1" :block-id 0}
        blk2 {:file-name "f1" :block-id 1}
        blk3 {:file-name "f1" :block-id 2}]
    (ensure-block-exists file-mgr blk1)
    (ensure-block-exists file-mgr blk2)
    (ensure-block-exists file-mgr blk3)
    (let [buf1 (.pin buffer-mgr blk1)
          buf2 (.pin buffer-mgr blk2)
          buf3 (.pin buffer-mgr blk3)]
      (.markDirty buf1 10 100)
      (.markDirty buf2 10 101)
      (.markDirty buf3 20 102)
      (.flushAll buffer-mgr 10)
      (is (= -1 (.txId buf1)))
      (is (= -1 (.txId buf2)))
      (is (= 20 (.txId buf3))))))

(deftest buffer-mgr-concurrent
  (let [{:keys [file-mgr buffer-mgr]} (setup 3)
        blk {:file-name "f1" :block-id 0}
        results (atom [])]
    (ensure-block-exists file-mgr blk)
    (let [threads (repeatedly 5
                              #(Thread.
                                (fn []
                                  (try
                                    (let [buf (.pin buffer-mgr blk)]
                                      (Thread/sleep 10)
                                      (.unpin buffer-mgr buf)
                                      (swap! results conj :success))
                                    (catch Exception _
                                      (swap! results conj :error))))))]
      (doseq [t threads] (.start t))
      (doseq [t threads] (.join t))
      (is (= 5 (count @results)))
      (is (every? #(= :success %) @results))
      (is (= 3 (.available buffer-mgr))))))
