(ns opusdb.benchmark.throughput
  (:require
   [criterium.core :as crit]
   [opusdb.atomic.stm :as stm]))

(defn benchmark-simple-increments
  "Measures throughput of simple counter increments"
  [n-refs n-txns]
  (let [refs (vec (repeatedly n-refs #(stm/ref 0)))
        start (System/nanoTime)]
    (dotimes [_ n-txns]
      (stm/dosync
       (doseq [r refs]
         (stm/alter r inc))))
    (let [elapsed-ms (/ (- (System/nanoTime) start) 1e6)
          txns-per-sec (/ n-txns (/ elapsed-ms 1000))
          final-values (mapv stm/deref refs)
          expected n-txns
          correct? (every? #(= % expected) final-values)]
      {:txns n-txns
       :refs n-refs
       :elapsed-ms (format "%.2f" elapsed-ms)
       :txns-per-sec (format "%.0f" txns-per-sec)
       :correct? correct?})))

(defn benchmark-contention
  "Measures throughput under high contention (multiple threads, single ref)"
  [n-threads n-txns-per-thread]
  (let [counter (stm/ref 0)
        latch (java.util.concurrent.CountDownLatch. n-threads)
        start-latch (java.util.concurrent.CountDownLatch. 1)
        threads (doall
                 (for [_ (range n-threads)]
                   (Thread.
                    (fn []
                      (.countDown latch)
                      (.await start-latch)
                      (dotimes [_ n-txns-per-thread]
                        (stm/dosync
                         (stm/alter counter inc)))))))]
    (doseq [^Thread t threads] (.start t))
    (.await latch)  ; Wait for all threads to be ready
    (let [start (System/nanoTime)]
      (.countDown start-latch)  ; Start all threads simultaneously
      (doseq [^Thread t threads] (.join t))
      (let [elapsed-ms (/ (- (System/nanoTime) start) 1e6)
            total-txns (* n-threads n-txns-per-thread)
            txns-per-sec (/ total-txns (/ elapsed-ms 1000))
            final-value (stm/deref counter)
            expected (* n-threads n-txns-per-thread)]
        {:threads n-threads
         :total-txns total-txns
         :elapsed-ms (format "%.2f" elapsed-ms)
         :txns-per-sec (format "%.0f" txns-per-sec)
         :final-value final-value
         :expected expected
         :correct? (= final-value expected)}))))

(defn benchmark-read-write-mix
  "Measures throughput with mixed read/write workload"
  [n-refs n-threads n-ops-per-thread write-ratio]
  (let [refs (vec (repeatedly n-refs #(stm/ref 0)))
        latch (java.util.concurrent.CountDownLatch. n-threads)
        start-latch (java.util.concurrent.CountDownLatch. 1)
        total-writes (atom 0)
        threads (doall
                 (for [_ (range n-threads)]
                   (Thread.
                    (fn []
                      (.countDown latch)
                      (.await start-latch)
                      (dotimes [_ n-ops-per-thread]
                        (if (< (rand) write-ratio)
                          (do
                            (swap! total-writes inc)
                            (stm/dosync
                             (let [r (rand-nth refs)]
                               (stm/alter r inc))))
                          (stm/dosync
                           (reduce + (map stm/deref refs)))))))))]
    (doseq [^Thread t threads] (.start t))
    (.await latch)
    (let [start (System/nanoTime)]
      (.countDown start-latch)
      (doseq [^Thread t threads] (.join t))
      (let [elapsed-ms (/ (- (System/nanoTime) start) 1e6)
            total-ops (* n-threads n-ops-per-thread)
            ops-per-sec (/ total-ops (/ elapsed-ms 1000))
            final-sum (reduce + (map stm/deref refs))
            writes @total-writes]
        {:threads n-threads
         :refs n-refs
         :total-ops total-ops
         :writes writes
         :reads (- total-ops writes)
         :write-ratio (format "%.1f%%" (* write-ratio 100))
         :elapsed-ms (format "%.2f" elapsed-ms)
         :ops-per-sec (format "%.0f" ops-per-sec)
         :final-sum final-sum
         :correct? (= final-sum writes)}))))

(defn benchmark-bank-transfer
  "Measures throughput with realistic bank transfer scenario"
  [n-accounts n-threads n-transfers-per-thread]
  (let [accounts (vec (repeatedly n-accounts #(stm/ref 1000)))
        latch (java.util.concurrent.CountDownLatch. n-threads)
        start-latch (java.util.concurrent.CountDownLatch. 1)
        successful-transfers (atom 0)
        threads (doall
                 (for [_ (range n-threads)]
                   (Thread.
                    (fn []
                      (.countDown latch)
                      (.await start-latch)
                      (dotimes [_ n-transfers-per-thread]
                        (let [from-idx (rand-int n-accounts)
                              to-idx (rand-int n-accounts)]
                          (when (not= from-idx to-idx)
                            (let [from (nth accounts from-idx)
                                  to (nth accounts to-idx)
                                  amount (inc (rand-int 100))
                                  success? (stm/dosync
                                            (when (>= (stm/deref from) amount)
                                              (stm/alter from - amount)
                                              (stm/alter to + amount)
                                              true))]
                              (when success?
                                (swap! successful-transfers inc))))))))))]
    (doseq [^Thread t threads] (.start t))
    (.await latch)
    (let [start (System/nanoTime)]
      (.countDown start-latch)
      (doseq [^Thread t threads] (.join t))
      (let [elapsed-ms (/ (- (System/nanoTime) start) 1e6)
            successful @successful-transfers
            txns-per-sec (/ successful (/ elapsed-ms 1000))
            total-balance (stm/dosync (reduce + (map stm/deref accounts)))
            expected-balance (* n-accounts 1000)]
        {:threads n-threads
         :accounts n-accounts
         :attempted-txns (* n-threads n-transfers-per-thread)
         :successful-txns successful
         :elapsed-ms (format "%.2f" elapsed-ms)
         :txns-per-sec (format "%.0f" txns-per-sec)
         :total-balance total-balance
         :expected-balance expected-balance
         :correct? (= total-balance expected-balance)}))))

(defn benchmark-low-contention
  "Measures throughput when each thread works on different refs (low contention)"
  [n-threads n-txns-per-thread]
  (let [refs (vec (repeatedly n-threads #(stm/ref 0)))
        latch (java.util.concurrent.CountDownLatch. n-threads)
        start-latch (java.util.concurrent.CountDownLatch. 1)
        threads (doall
                 (for [i (range n-threads)]
                   (Thread.
                    (fn []
                      (.countDown latch)
                      (.await start-latch)
                      (let [my-ref (nth refs i)]
                        (dotimes [_ n-txns-per-thread]
                          (stm/dosync
                           (stm/alter my-ref inc))))))))]
    (doseq [^Thread t threads] (.start t))
    (.await latch)
    (let [start (System/nanoTime)]
      (.countDown start-latch)
      (doseq [^Thread t threads] (.join t))
      (let [elapsed-ms (/ (- (System/nanoTime) start) 1e6)
            total-txns (* n-threads n-txns-per-thread)
            txns-per-sec (/ total-txns (/ elapsed-ms 1000))
            final-values (mapv stm/deref refs)
            correct? (every? #(= % n-txns-per-thread) final-values)]
        {:threads n-threads
         :total-txns total-txns
         :elapsed-ms (format "%.2f" elapsed-ms)
         :txns-per-sec (format "%.0f" txns-per-sec)
         :contention "low"
         :correct? correct?}))))

(defn benchmark-with-criterium
  "Uses criterium for statistical benchmarking"
  []
  (println "\n=== Criterium Statistical Benchmarks ===\n")

  (println "Single-threaded increment:")
  (let [r (stm/ref 0)]
    (crit/quick-bench
     (stm/dosync
      (stm/alter r inc))))

  (println "\nSingle ref-set:")
  (let [r (stm/ref 0)]
    (crit/quick-bench
     (stm/dosync
      (stm/ref-set r 42))))

  (println "\nRead-only transaction (5 refs):")
  (let [refs (vec (repeatedly 5 #(stm/ref 0)))]
    (crit/quick-bench
     (stm/dosync
      (reduce + (map stm/deref refs)))))

  (println "\nRead-only transaction (10 refs):")
  (let [refs (vec (repeatedly 10 #(stm/ref 0)))]
    (crit/quick-bench
     (stm/dosync
      (reduce + (map stm/deref refs)))))

  (println "\nWrite transaction (5 refs):")
  (let [refs (vec (repeatedly 5 #(stm/ref 0)))]
    (crit/quick-bench
     (stm/dosync
      (doseq [r refs]
        (stm/alter r inc))))))

(defn run-all-benchmarks []
  (println "\n=== Throughput Benchmarks ===\n")

  (println "1. Simple Increments (single-threaded, 10 refs):")
  (println (benchmark-simple-increments 10 10000))

  (println "\n2. High Contention (multiple threads, single ref):")
  (println "  4 threads:")
  (println "   " (benchmark-contention 4 10000))
  (println "  8 threads:")
  (println "   " (benchmark-contention 8 10000))
  (println "  16 threads:")
  (println "   " (benchmark-contention 16 10000))

  (println "\n3. Low Contention (isolated refs per thread):")
  (println "  4 threads:")
  (println "   " (benchmark-low-contention 4 10000))
  (println "  8 threads:")
  (println "   " (benchmark-low-contention 8 10000))
  (println "  16 threads:")
  (println "   " (benchmark-low-contention 16 10000))

  (println "\n4. Read-Heavy Mix (10% writes, 10 refs):")
  (println "  4 threads:")
  (println "   " (benchmark-read-write-mix 10 4 5000 0.1))
  (println "  8 threads:")
  (println "   " (benchmark-read-write-mix 10 8 5000 0.1))

  (println "\n5. Balanced Mix (50% writes, 10 refs):")
  (println "  4 threads:")
  (println "   " (benchmark-read-write-mix 10 4 5000 0.5))
  (println "  8 threads:")
  (println "   " (benchmark-read-write-mix 10 8 5000 0.5))

  (println "\n6. Write-Heavy Mix (90% writes, 10 refs):")
  (println "  4 threads:")
  (println "   " (benchmark-read-write-mix 10 4 5000 0.9))
  (println "  8 threads:")
  (println "   " (benchmark-read-write-mix 10 8 5000 0.9))

  (println "\n7. Bank Transfer (realistic workload, 20 accounts):")
  (println "  4 threads:")
  (println "   " (benchmark-bank-transfer 20 4 5000))
  (println "  8 threads:")
  (println "   " (benchmark-bank-transfer 20 8 5000))
  (println "  16 threads:")
  (println "   " (benchmark-bank-transfer 20 16 5000))

  (benchmark-with-criterium))

(run-all-benchmarks)