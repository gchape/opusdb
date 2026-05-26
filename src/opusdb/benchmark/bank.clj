(ns opusdb.benchmark.bank
  (:require
   [criterium.core :as crit]
   [opusdb.atomic.stm :as stm])
  (:import
   [java.util.concurrent CountDownLatch ThreadLocalRandom]
   [java.util.concurrent.atomic LongAdder]))

(defn make-bank [n-accounts initial-balance]
  {:accounts  (vec (repeatedly n-accounts #(stm/ref initial-balance)))
   :transfers (LongAdder.)})

(defn transfer
  "Realistic transfer: commits only when the source account has sufficient
   funds. Used for throughput and correctness benchmarks where business
   logic must be exercised under concurrent load."
  [bank from to amount]
  (stm/dosync
   (let [from-ref ((:accounts bank) from)
         to-ref   ((:accounts bank) to)
         from-bal (stm/deref from-ref)]
     (when (>= from-bal amount)
       (stm/ref-set from-ref (- from-bal amount))
       (stm/ref-set to-ref   (+ (stm/deref to-ref) amount))
       (stm/on-commit #(.increment ^LongAdder (:transfers bank)))
       true))))

(defn- transfer-unchecked
  "Latency-benchmark variant: skips the balance guard so the STM commit path
   is always exercised regardless of account state. Criterium runs hundreds of
   thousands of iterations on the same refs — a realistic transfer would drain
   the balance early and reduce subsequent calls to a cheap nil return, making
   the measurement a blend of commit overhead and failed-check overhead rather
   than a clean signal. Disclosed in the thesis experimental setup section."
  [bank from to amount]
  (stm/dosync
   (let [from-ref ((:accounts bank) from)
         to-ref   ((:accounts bank) to)]
     (stm/ref-set from-ref (- (stm/deref from-ref) amount))
     (stm/ref-set to-ref   (+ (stm/deref to-ref) amount))
     (stm/on-commit #(.increment ^LongAdder (:transfers bank)))
     true)))

(defn- transfer-unchecked-no-hook
  "Same as transfer-unchecked but registers no on-commit hook.
   Used as the baseline in the hook-overhead comparison so the only
   difference between the two measurements is hook dispatch cost."
  [bank from to amount]
  (stm/dosync
   (let [from-ref ((:accounts bank) from)
         to-ref   ((:accounts bank) to)]
     (stm/ref-set from-ref (- (stm/deref from-ref) amount))
     (stm/ref-set to-ref   (+ (stm/deref to-ref) amount))
     true)))

(defn total-balance [bank]
  (stm/dosync (reduce + (map stm/deref (:accounts bank)))))

(defn- launch-threads [n f]
  (let [ready   (CountDownLatch. n)
        gate    (CountDownLatch. 1)
        threads (into-array Thread
                            (for [i (range n)]
                              (Thread.
                               ^Runnable
                               (fn []
                                 (let [tlr (ThreadLocalRandom/current)]
                                   (.countDown ready)
                                   (.await ^CountDownLatch gate)
                                   (f i tlr))))))]
    (doseq [^Thread t threads] (.start t))
    (.await ready)
    [threads gate]))

(defn- join-all [threads]
  (doseq [^Thread t threads] (.join t)))

(defn- run-bank-throughput
  "Concurrent throughput measurement using realistic transfer.
   Initial balance of 100 000 keeps rejection rate negligible across the
   bounded transaction count, so TPS reflects contention overhead not
   business-logic failures."
  [n-threads n-accounts n-txns-per-thread]
  (let [bank       (make-bank n-accounts 100000)
        successful (LongAdder.)
        start      (volatile! 0)
        [threads ^CountDownLatch gate]
        (launch-threads
         n-threads
         (fn [_ ^ThreadLocalRandom tlr]
           (dotimes [_ n-txns-per-thread]
             (let [from (.nextInt tlr n-accounts)
                   to   (int (mod (+ from 1 (.nextInt tlr (dec n-accounts)))
                                  n-accounts))
                   amt  (inc (.nextInt tlr 100))]
               (when (transfer bank from to amt)
                 (.increment successful))))))]
    (vreset! start (System/nanoTime))
    (.countDown gate)
    (join-all threads)
    (let [elapsed-ms (/ (- (System/nanoTime) @start) 1e6)
          succ       (.sum successful)
          correct?   (= (total-balance bank) (* n-accounts 100000))]
      {:threads      n-threads
       :successful   succ
       :elapsed-ms   elapsed-ms
       :txns-per-sec (long (/ succ (/ elapsed-ms 1000)))
       :correct?     correct?})))

(defn run-bank-throughput-suite []
  (println "\n--- Bank Transfer (20 accounts) ---")
  (doseq [n [4 6 12]]
    (println (str "\n  " n " threads:"))
    (run-bank-throughput n 20 200)   ; warm-up
    (let [{:keys [txns-per-sec correct?]} (run-bank-throughput n 20 3000)]
      (println (format "  opusdb  txns/sec: %d  correct: %s"
                       txns-per-sec (str correct?))))))

(defn run-correctness-check []
  (println "\n=== Bank Correctness Check ===")
  (let [n-accounts 10
        bank       (make-bank n-accounts 1000)
        running    (atom true)
        threads    (mapv (fn [_]
                           (Thread.
                            ^Runnable
                            (fn []
                              (let [tlr (ThreadLocalRandom/current)]
                                (while @running
                                  (let [from (.nextInt tlr n-accounts)
                                        to   (int (mod (+ from 1
                                                          (.nextInt tlr (dec n-accounts)))
                                                       n-accounts))
                                        amt  (inc (.nextInt tlr 50))]
                                    (transfer bank from to amt)))))))
                         (range 20))]
    (run! #(.start ^Thread %) threads)
    (Thread/sleep 5000)
    (reset! running false)
    (run! #(.join ^Thread % 2000) threads)
    (println (format "Total (should be %d): %d"
                     (* n-accounts 1000)
                     (total-balance bank)))
    (println (format "Successful transfers: %d"
                     (.sum ^LongAdder (:transfers bank))))))

(defn run-bank-latency-benchmarks []
  (println "\n=== Criterium Statistical Benchmarks ===")
  (println "\n--- Bank Latency ---")

  ;; (i) Single uncontended transfer — baseline commit path cost.
  ;; Criterium runs sequentially on one thread; this measures the STM's
  ;; commit overhead with zero contention.
  (println "\nBenchmarking single transfer transaction — opusdb:")
  (let [bank (make-bank 10 100000)]
    (crit/quick-bench (transfer-unchecked bank 0 1 10)))

  ;; (ii) Extreme contention: 12 futures all targeting the same two accounts
  ;;      concurrently. Measures the backoff and ownership-stealing path under
  ;;      peak multi-threaded pressure. Thread count matches benchmark ceiling.
  (println "\nBenchmarking extreme-contention with futures — opusdb:")
  (let [bank (make-bank 10 100000)]
    (crit/quick-bench
     (let [fs (mapv (fn [_] (future (transfer-unchecked bank 0 1 1))) (range 12))]
       (run! deref fs))))

  ;; (iii) on-commit hook overhead: isolates event-system dispatch cost by
  ;;       comparing identical STM work with and without a registered handler.
  ;;       This is the thesis's key contribution over native Clojure STM.
  (println "\nBenchmarking on-commit hook overhead (no hook baseline) — opusdb:")
  (let [bank (make-bank 10 100000)]
    (crit/quick-bench (transfer-unchecked-no-hook bank 0 1 1)))

  (println "\nBenchmarking on-commit hook overhead (with hook) — opusdb:")
  (let [bank (make-bank 10 100000)]
    (crit/quick-bench (transfer-unchecked bank 0 1 1))))

(defn benchmark-bank []
  (run-correctness-check)
  (run-bank-latency-benchmarks))