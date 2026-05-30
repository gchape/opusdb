(ns opusdb.benchmark.bank
  (:require [criterium.core :as crit]
            [opusdb.atomic.stm :as stm])
  (:import
   [java.util.concurrent CountDownLatch ThreadLocalRandom]
   [java.util.concurrent.atomic LongAdder]))

;; Thread counts used by both the suite runner and throughput.clj.
;; Defined here (the bank ns is the lower-level dependency) so
;; throughput.clj can refer to this var instead of duplicating the literal.
(def thread-counts [4 6 12])

(defn make-bank [n-accounts initial-balance]
  {:accounts  (vec (repeatedly n-accounts #(stm/ref initial-balance)))
   :transfers (LongAdder.)})

(defn transfer
  "Realistic transfer: commits only when the source account has sufficient
   funds. on-commit hook increments the counter atomically with the commit."
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
  "Latency variant: skips balance guard so every Criterium iteration
   exercises the full commit path regardless of account state."
  [bank from to amount]
  (stm/dosync
   (let [from-ref ((:accounts bank) from)
         to-ref   ((:accounts bank) to)]
     (stm/ref-set from-ref (- (stm/deref from-ref) amount))
     (stm/ref-set to-ref   (+ (stm/deref to-ref) amount))
     (stm/on-commit #(.increment ^LongAdder (:transfers bank)))
     true)))

(defn- transfer-unchecked-no-hook
  "Same as transfer-unchecked but with no on-commit hook.
   Baseline for isolating hook dispatch overhead."
  [bank from to amount]
  (stm/dosync
   (let [from-ref ((:accounts bank) from)
         to-ref   ((:accounts bank) to)]
     (stm/ref-set from-ref (- (stm/deref from-ref) amount))
     (stm/ref-set to-ref   (+ (stm/deref to-ref) amount))
     true)))

(defn total-balance [bank]
  (stm/dosync (reduce + (mapv stm/deref (:accounts bank)))))

;; ---------------------------------------------------------------------------
;; Thread utilities
;; ---------------------------------------------------------------------------

(defn- launch-threads [n f]
  (let [ready   (CountDownLatch. n)
        gate    (CountDownLatch. 1)
        threads (into-array
                 Thread
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

;; ---------------------------------------------------------------------------
;; Throughput benchmark
;;
;; Initial balance of 100,000 keeps rejection rate negligible across the
;; bounded transaction count, so TPS reflects contention overhead rather
;; than business-logic failures.
;;
;; No manual warmup pass — the JVM warms naturally across thread-count
;; iterations (4 → 6 → 12) and via the history-sweep that precedes this.
;; ---------------------------------------------------------------------------

(defn run-bank-throughput
  "Returns a result map.  Called by both the suite runner and the history sweep."
  [n-threads n-accounts n-txns-per-thread]
  (let [bank       (make-bank n-accounts 100000)
        successful (LongAdder.)
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
                 (.increment successful))))))
        ;; FIX: start the clock when threads are actually released, matching
        ;;      the timed-macro pattern used in throughput.clj.
        start      (do (.countDown gate) (System/nanoTime))
        _          (join-all threads)
        elapsed-ms (/ (- (System/nanoTime) start) 1e6)
        succ       (.sum successful)
        correct?   (= (total-balance bank) (* n-accounts 100000))]
    {:threads      n-threads
     :successful   succ
     :elapsed-ms   elapsed-ms
     :txns-per-sec (long (/ succ (/ elapsed-ms 1000)))
     :correct?     correct?}))

;; FIX: added run-bank-throughput-counted, referenced by throughput.clj's
;;      history sweep.  Accepts a dosync wrapper so abort counting is
;;      handled externally by the caller (throughput/counting-dosync).
(defn run-bank-throughput-counted
  "Like run-bank-throughput but accepts a custom dosync* wrapper
   (e.g. counting-dosync) for abort instrumentation.
   Returns the same result map as run-bank-throughput."
  [dosync* n-threads n-accounts n-txns-per-thread]
  (let [bank       (make-bank n-accounts 100000)
        successful (LongAdder.)
        [threads ^CountDownLatch gate]
        (launch-threads
         n-threads
         (fn [_ ^ThreadLocalRandom tlr]
           (dotimes [_ n-txns-per-thread]
             (let [from (.nextInt tlr n-accounts)
                   to   (int (mod (+ from 1 (.nextInt tlr (dec n-accounts)))
                                  n-accounts))
                   amt  (inc (.nextInt tlr 100))]
               (dosync*
                (fn []
                  (let [from-ref ((:accounts bank) from)
                        to-ref   ((:accounts bank) to)
                        from-bal (stm/deref from-ref)]
                    (when (>= from-bal amt)
                      (stm/ref-set from-ref (- from-bal amt))
                      (stm/ref-set to-ref   (+ (stm/deref to-ref) amt))
                      (stm/on-commit #(.increment successful))
                      true))))))))
        start      (do (.countDown gate) (System/nanoTime))
        _          (join-all threads)
        elapsed-ms (/ (- (System/nanoTime) start) 1e6)
        succ       (.sum successful)
        correct?   (= (total-balance bank) (* n-accounts 100000))]
    {:threads      n-threads
     :successful   succ
     :elapsed-ms   elapsed-ms
     :txns-per-sec (long (/ succ (/ elapsed-ms 1000)))
     :correct?     correct?}))

(defn run-bank-throughput-suite []
  (println "\n--- Bank Transfer (20 accounts) ---")
  ;; FIX: use the shared thread-counts var instead of a duplicated literal.
  (doseq [n thread-counts]
    (println (str "\n  " n " threads:"))
    (let [{:keys [txns-per-sec correct?]} (run-bank-throughput n 20 3000)]
      (println (format "  opusdb  txns/sec: %d  correct: %s"
                       txns-per-sec (str correct?))))))

;; ---------------------------------------------------------------------------
;; Correctness check
;; ---------------------------------------------------------------------------

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
    ;; FIX: check whether each thread actually finished within the timeout
    ;;      and warn if any are still alive (rather than silently continuing).
    (doseq [^Thread t threads]
      (.join t 2000)
      (when (.isAlive t)
        (println "  WARNING: worker thread did not terminate within timeout")))
    (println (format "Total (should be %d): %d"
                     (* n-accounts 1000)
                     (total-balance bank)))
    (println (format "Successful transfers: %d"
                     (.sum ^LongAdder (:transfers bank))))))

;; ---------------------------------------------------------------------------
;; Latency benchmarks (Criterium, single-threaded)
;;
;; No manual warmup — Criterium handles warmup internally via its
;; estimation and warmup phases.
;;
;; Four scenarios:
;;   (i)   Single uncontended transfer — floor cost of the full commit path.
;;   (ii)  Extreme contention (12 futures) — backoff + ownership-stealing.
;;   (iii) No-hook baseline — isolates hook dispatch cost.
;;   (iv)  With on-commit hook — same work + hook dispatch.
;; ---------------------------------------------------------------------------

(defn run-bank-latency-benchmarks []
  (println "\n=== Criterium Statistical Benchmarks ===")
  (println "\n--- Bank Latency ---")

  (println "\nBenchmarking single transfer transaction — opusdb:")
  (let [bank (make-bank 10 100000)]
    (crit/quick-bench (transfer-unchecked bank 0 1 10)))

  (println "\nBenchmarking extreme-contention with futures — opusdb:")
  (let [bank (make-bank 10 100000)]
    (crit/quick-bench
     (let [fs (mapv (fn [_] (future (transfer-unchecked bank 0 1 1))) (range 12))]
       (run! deref fs))))

  (println "\nBenchmarking on-commit hook overhead (no hook baseline) — opusdb:")
  (let [bank (make-bank 10 100000)]
    (crit/quick-bench (transfer-unchecked-no-hook bank 0 1 1)))

  (println "\nBenchmarking on-commit hook overhead (with hook) — opusdb:")
  (let [bank (make-bank 10 100000)]
    (crit/quick-bench (transfer-unchecked bank 0 1 1))))

(defn benchmark-bank []
  (run-correctness-check)
  (run-bank-throughput-suite)
  (run-bank-latency-benchmarks))