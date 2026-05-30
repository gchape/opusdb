(ns opusdb.benchmark.throughput
  (:require
   [opusdb.atomic.stm :as stm]
   [opusdb.benchmark.bank :as bank])
  (:import
   [java.util.concurrent CountDownLatch ThreadLocalRandom]
   [java.util.concurrent.atomic LongAdder]))

(def ^:private thread-counts bank/thread-counts)

(def ^:private sweep-thread-counts [1 2 4 6 12])
(def ^:private sweep-history-sizes [8 16 32 64 128 256])
(def ^:private sweep-txns-per-thread 50000)

;; Abort accounting
(def ^:private ^LongAdder conflict-aborts (LongAdder.))
(def ^:private ^LongAdder trim-aborts     (LongAdder.))

(defn- reset-abort-counters! []
  (.reset conflict-aborts)
  (.reset trim-aborts))

(defn- conflict-abort-count [] (.sum conflict-aborts))
(defn- trim-abort-count     [] (.sum trim-aborts))

(defn- counting-dosync
  "Wraps dosync and tallies conflict aborts by counting extra attempts.
   Trim aborts are recorded via stm/*trim-abort-hook*."
  [f]
  (let [attempts (volatile! 0)]
    (stm/dosync
     (vswap! attempts inc)
     (f))
    (let [extra (dec @attempts)]
      (when (pos? extra)
        (.add conflict-aborts (long extra))))))

;; Timing / thread utilities

(defmacro ^:private timed [& body]
  `(let [start# (System/nanoTime)
         _#     (do ~@body)]
     (/ (- (System/nanoTime) start#) 1e6)))

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

;; Scenario runners

(defn- run-contention
  [dosync* n-threads n-txns]
  (reset-abort-counters!)
  (let [counter (stm/ref 0)
        [threads ^CountDownLatch gate]
        (launch-threads n-threads
                        (fn [_ _]
                          (dotimes [_ n-txns]
                            (dosync* #(stm/alter counter inc)))))
        elapsed (timed (.countDown gate) (join-all threads))
        total   (* n-threads n-txns)
        final   (stm/deref counter)]
    {:threads                    n-threads
     :total-txns                 total
     :elapsed-ms                 (format "%.2f" elapsed)
     :txns-per-sec               (format "%.0f" (/ total (/ elapsed 1000)))
     :conflict-aborts-per-commit (format "%.2f" (/ (double (conflict-abort-count)) (double total)))
     :trim-aborts-per-commit     (format "%.2f" (/ (double (trim-abort-count)) (double total)))
     :final-value                final
     :expected                   total
     :correct?                   (= final total)}))

(defn- run-low-contention
  [n-threads n-txns]
  (let [refs    (vec (repeatedly n-threads #(stm/ref 0)))
        [threads ^CountDownLatch gate]
        (launch-threads n-threads
                        (fn [i _]
                          (let [r (nth refs i)]
                            (dotimes [_ n-txns]
                              (stm/dosync (stm/alter r inc))))))
        elapsed (timed (.countDown gate) (join-all threads))
        total   (* n-threads n-txns)
        finals  (mapv stm/deref refs)]
    {:threads      n-threads
     :total-txns   total
     :elapsed-ms   (format "%.2f" elapsed)
     :txns-per-sec (format "%.0f" (/ total (/ elapsed 1000)))
     :correct?     (every? #(= % n-txns) finals)}))

(defn- run-read-write-mix
  [n-refs n-threads n-ops write-ratio]
  (reset-abort-counters!)
  (let [refs         (vec (repeatedly n-refs #(stm/ref 0)))
        n-refs-int   (int n-refs)
        total-writes (LongAdder.)
        total-reads  (LongAdder.)
        [threads ^CountDownLatch gate]
        (launch-threads
         n-threads
         (fn [_ ^ThreadLocalRandom tlr]
           (dotimes [_ n-ops]
             (if (< (.nextDouble tlr) write-ratio)
               (do (.increment total-writes)
                   (stm/dosync
                    (stm/alter (nth refs (.nextInt tlr n-refs-int)) inc)))
               (do (.increment total-reads)
                   (stm/dosync (reduce + (mapv stm/deref refs))))))))
        elapsed   (timed (.countDown gate) (join-all threads))
        total-ops (* n-threads n-ops)
        writes    (.sum total-writes)
        final-sum (reduce + (mapv stm/deref refs))]
    {:threads                    n-threads
     :refs                       n-refs
     :total-ops                  total-ops
     :writes                     writes
     :reads                      (.sum total-reads)
     :write-ratio                (format "%.0f%%" (* write-ratio 100))
     :elapsed-ms                 (format "%.2f" elapsed)
     :txns-per-sec               (format "%.0f" (/ total-ops (/ elapsed 1000)))
     :conflict-aborts-per-commit (when (pos? writes)
                                   (format "%.2f" (/ (double (conflict-abort-count))
                                                     (double writes))))
     :trim-aborts-per-commit     (when (pos? writes)
                                   (format "%.2f" (/ (double (trim-abort-count))
                                                     (double writes))))
     :final-sum                  final-sum
     :correct?                   (= final-sum writes)}))

(defn run-history-sweep
  "Version-history size sweep (bank transfer, 20 accounts).
   Runs all (history-size x thread-count) cells with sweep-txns-per-thread txns/thread.
   Safe to call in isolation: lein run -m opusdb.benchmark.throughput/run-history-sweep"
  []
  (println "\n--- Version History Size Sweep (bank transfer, 20 accounts) ---")
  (println (format "  accounts=20  txns-per-thread=%d" sweep-txns-per-thread))
  (println (format "  %-12s %-8s %-14s %-24s %-22s"
                   "max-history" "threads" "txns/sec"
                   "conflict-aborts/commit" "trim-aborts/commit"))
  (doseq [h sweep-history-sizes
          n sweep-thread-counts]
    (reset-abort-counters!)
    (stm/with-max-history h
      (binding [stm/*trim-abort-hook* #(.increment trim-aborts)]
        (let [{:keys [txns-per-sec successful correct?]}
              (bank/run-bank-throughput-counted
               counting-dosync n 20 sweep-txns-per-thread)
              committed (double (max 1 (or successful 1)))]
          (println (format "  %-12d %-8d %-14d %-24s %-22s"
                           h n txns-per-sec
                           (format "%.2f" (/ (double (conflict-abort-count)) committed))
                           (format "%.2f" (/ (double (trim-abort-count)) committed))))
          (when-not correct?
            (println "  *** CORRECTNESS FAILURE ***")))))))

(defn run-throughput-benchmarks []
  (println "\n=== Throughput Benchmarks ===")

  (println "\n--- High Contention (single ref) ---")
  (doseq [n thread-counts]
    (println (str "\n  " n " threads:"))
    (let [r (run-contention counting-dosync n 10000)]
      (println (format "  opusdb  txns/sec: %s  conflict-aborts/commit: %s  trim-aborts/commit: %s  correct: %s"
                       (:txns-per-sec r)
                       (:conflict-aborts-per-commit r)
                       (:trim-aborts-per-commit r)
                       (:correct? r)))))

  (println "\n--- Low Contention (isolated refs) ---")
  (doseq [n thread-counts]
    (println (str "\n  " n " threads:"))
    (let [r (run-low-contention n 10000)]
      (println (format "  opusdb  txns/sec: %s  correct: %s"
                       (:txns-per-sec r) (:correct? r)))))

  (bank/run-bank-throughput-suite)

  (println "\n--- Read-Heavy Mix (10% writes, 10 refs) ---")
  (doseq [n thread-counts]
    (println (str "\n  " n " threads:"))
    (let [r (run-read-write-mix 10 n 5000 0.1)]
      (println (format "  opusdb  txns/sec: %s  correct: %s"
                       (:txns-per-sec r) (:correct? r)))))

  (println "\n--- Write-Heavy Mix (90% writes, 10 refs) ---")
  (doseq [n thread-counts]
    (println (str "\n  " n " threads:"))
    (let [r (run-read-write-mix 10 n 5000 0.9)]
      (println (format "  opusdb  txns/sec: %s  conflict-aborts/commit: %s  trim-aborts/commit: %s  correct: %s"
                       (:txns-per-sec r)
                       (:conflict-aborts-per-commit r)
                       (:trim-aborts-per-commit r)
                       (:correct? r)))))

  (run-history-sweep))

(defn run-all-benchmarks []
  (run-throughput-benchmarks))