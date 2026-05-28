(ns opusdb.benchmark.throughput
  (:require
   [criterium.core :as crit]
   [opusdb.atomic.stm :as stm]
   [opusdb.benchmark.bank :as bank])
  (:import
   [java.util.concurrent CountDownLatch ThreadLocalRandom]
   [java.util.concurrent.atomic LongAdder]))

;; Thread counts: 4 (below physical core count), 6 (at core limit on the
;; benchmark machine), 12 (2× cores — mild oversubscription). This spread
;; isolates STM contention from OS scheduler noise at 16+ threads.
(def ^:private thread-counts [4 6 12])

;; ---------------------------------------------------------------------------
;; Abort counting
;;
;; Uses volatile! instead of atom — single-threaded per call site, so no
;; CAS overhead needed. After a successful commit, (attempts - 1) equals
;; the number of aborts for that transaction.
;; ---------------------------------------------------------------------------

(def ^:private ^LongAdder bench-aborts (LongAdder.))

(defn- reset-aborts! [] (.reset bench-aborts))
(defn- abort-count   [] (.sum   bench-aborts))

(defn- counting-dosync [f]
  (let [attempts (volatile! 0)]
    (stm/dosync
     (vswap! attempts inc)
     (f))
    (let [extra (dec @attempts)]
      (when (pos? extra)
        (.add bench-aborts (long extra))))))

;; ---------------------------------------------------------------------------
;; Timing and thread utilities
;; ---------------------------------------------------------------------------

(defmacro ^:private timed [& body]
  `(let [start# (System/nanoTime)
         result# (do ~@body)]
     [result# (/ (- (System/nanoTime) start#) 1e6)]))

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

;; ---------------------------------------------------------------------------
;; Benchmark scenarios
;; ---------------------------------------------------------------------------

(defn- run-contention
  "High contention: all threads increment a single shared ref.
   Worst case for optimistic concurrency — every pair conflicts."
  [dosync n-threads n-txns]
  (reset-aborts!)
  (let [counter (stm/ref 0)
        [threads ^CountDownLatch gate]
        (launch-threads n-threads
                        (fn [_ _tlr]
                          (dotimes [_ n-txns]
                            (dosync #(stm/alter counter inc)))))
        [_ elapsed] (timed (.countDown gate) (join-all threads))
        total   (* n-threads n-txns)
        final   (stm/deref counter)]
    {:threads           n-threads
     :total-txns        total
     :elapsed-ms        (format "%.2f" elapsed)
     :txns-per-sec      (format "%.0f" (/ total (/ elapsed 1000)))
     :aborts-per-commit (format "%.2f" (/ (double (abort-count)) (double total)))
     :final-value       final
     :expected          total
     :correct?          (= final total)}))

(defn- run-low-contention
  "Low contention: each thread increments its own isolated ref.
   No conflicts by construction — measures baseline commit path cost."
  [n-threads n-txns]
  (let [refs    (vec (repeatedly n-threads #(stm/ref 0)))
        [threads ^CountDownLatch gate]
        (launch-threads n-threads
                        (fn [i _tlr]
                          (let [r (nth refs i)]
                            (dotimes [_ n-txns]
                              (stm/dosync (stm/alter r inc))))))
        [_ elapsed] (timed (.countDown gate) (join-all threads))
        total   (* n-threads n-txns)
        finals  (mapv stm/deref refs)]
    {:threads      n-threads
     :total-txns   total
     :elapsed-ms   (format "%.2f" elapsed)
     :txns-per-sec (format "%.0f" (/ total (/ elapsed 1000)))
     :correct?     (every? #(= % n-txns) finals)}))

(defn- run-read-write-mix
  "Mixed workload: write-ratio fraction of transactions write one random ref;
   the rest read-sum all refs. Tests MVCC read-path advantage."
  [n-refs n-threads n-ops write-ratio]
  (reset-aborts!)
  (let [refs         (vec (repeatedly n-refs #(stm/ref 0)))
        n-refs-int   (int n-refs)
        total-writes (LongAdder.)
        total-reads  (LongAdder.)
        [threads ^CountDownLatch gate]
        (launch-threads n-threads
                        (fn [_ ^ThreadLocalRandom tlr]
                          (dotimes [_ n-ops]
                            (if (< (.nextDouble tlr) write-ratio)
                              (do (.increment total-writes)
                                  (let [idx (.nextInt tlr n-refs-int)]
                                    (stm/dosync (stm/alter (nth refs idx) inc))))
                              (do (.increment total-reads)
                                  (stm/dosync (reduce + (mapv stm/deref refs))))))))
        [_ elapsed]  (timed (.countDown gate) (join-all threads))
        total-ops    (* n-threads n-ops)
        writes       (.sum total-writes)
        final-sum    (reduce + (mapv stm/deref refs))
        aborts-per-write-commit
        (when (pos? writes)
          (format "%.2f" (/ (double (abort-count)) (double writes))))]
    {:threads           n-threads
     :refs              n-refs
     :total-ops         total-ops
     :writes            writes
     :reads             (.sum total-reads)
     :write-ratio       (format "%.0f%%" (* write-ratio 100))
     :elapsed-ms        (format "%.2f" elapsed)
     :txns-per-sec      (format "%.0f" (/ total-ops (/ elapsed 1000)))
     :aborts-per-commit aborts-per-write-commit
     :final-sum         final-sum
     :correct?          (= final-sum writes)}))

;; ---------------------------------------------------------------------------
;; Scenario runners — warm up once, then record
;; ---------------------------------------------------------------------------

(defn- run-scenario [label f]
  (println (str "\n" label))
  (f)   ; warm-up
  (let [result (f)]
    (println (format "  opusdb  txns/sec: %s  correct: %s"
                     (:txns-per-sec result) (:correct? result)))))

(defn- run-scenario-counted [label f]
  (println (str "\n" label))
  (f)   ; warm-up
  (let [result (f)]
    (println (format "  opusdb  txns/sec: %s  aborts/commit: %s  correct: %s"
                     (:txns-per-sec result)
                     (or (:aborts-per-commit result) "n/a")
                     (:correct? result)))))

;; ---------------------------------------------------------------------------
;; Criterium micro-benchmarks
;;
;; These run single-threaded to isolate specific path costs:
;;   - single increment:    baseline write-transaction cost
;;   - single ref-set:      alternative write primitive cost
;;   - read-only N refs:    read-path cost vs ref count
;;   - write N refs:        multi-ref write transaction cost
;; ---------------------------------------------------------------------------

(defn- run-criterium-benchmarks []
  (println "\n=== Criterium Statistical Benchmarks ===")
  (doseq [[label thunk]
          [["Single increment"
            (let [r (stm/ref 0)]
              #(stm/dosync (stm/alter r inc)))]
           ["Single ref-set"
            (let [r (stm/ref 0)]
              #(stm/dosync (stm/ref-set r 42)))]
           ["Read-only 5 refs"
            (let [rs (vec (repeatedly 5 #(stm/ref 0)))]
              #(stm/dosync (reduce + (mapv stm/deref rs))))]
           ["Read-only 10 refs"
            (let [rs (vec (repeatedly 10 #(stm/ref 0)))]
              #(stm/dosync (reduce + (mapv stm/deref rs))))]
           ["Write 5 refs"
            (let [rs (vec (repeatedly 5 #(stm/ref 0)))]
              #(stm/dosync (doseq [r rs] (stm/alter r inc))))]]]
    (println (str "\n" label " — opusdb:"))
    (crit/quick-bench (thunk))))

(defn run-throughput-benchmarks []
  (println "\n=== Throughput Benchmarks ===")

  (println "\n--- High Contention (single ref) ---")
  (doseq [n thread-counts]
    (run-scenario-counted (str "  " n " threads:")
                          #(run-contention counting-dosync n 10000)))

  (println "\n--- Low Contention (isolated refs) ---")
  (doseq [n thread-counts]
    (run-scenario (str "  " n " threads:")
                  #(run-low-contention n 10000)))

  (bank/run-bank-throughput-suite)

  (println "\n--- Read-Heavy Mix (10% writes, 10 refs) ---")
  (doseq [n thread-counts]
    (run-scenario (str "  " n " threads:")
                  #(run-read-write-mix 10 n 5000 0.1)))

  (println "\n--- Write-Heavy Mix (90% writes, 10 refs) ---")
  (doseq [n thread-counts]
    (run-scenario-counted (str "  " n " threads:")
                          #(run-read-write-mix 10 n 5000 0.9))))

(defn run-all-benchmarks []
  (run-throughput-benchmarks)
  (run-criterium-benchmarks))