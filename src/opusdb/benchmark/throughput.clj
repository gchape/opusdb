(ns opusdb.benchmark.throughput
  (:require
   [criterium.core :as crit]
   [opusdb.atomic.stm :as stm]
   [opusdb.benchmark.bank :as bank])
  (:import
   [java.util.concurrent CountDownLatch ThreadLocalRandom]
   [java.util.concurrent.atomic LongAdder]))

;; Thread counts: 4 (below physical core count), 6 (at core limit on the
;; benchmark machine), 12 (2× cores — mild oversubscription).  This spread
;; isolates STM contention from OS scheduler noise at 16+ threads.
(def ^:private thread-counts [4 6 12])

;; ---------------------------------------------------------------------------
;; Abort counting — no STM internals modified.
;;
;; counting-dosync wraps each transaction in an atom-local attempt counter.
;; Because dosync re-executes the body on every retry, swap! runs once per
;; attempt.  After a successful commit, (attempts - 1) equals the number of
;; aborts for that transaction.  The atom is allocated per-call so there is
;; no cross-thread interference.
;; ---------------------------------------------------------------------------

(def ^:private ^LongAdder bench-aborts (LongAdder.))

(defn- reset-aborts! [] (.reset bench-aborts))
(defn- abort-count   [] (.sum   bench-aborts))

(defn- counting-dosync [f]
  (let [attempts (atom 0)]
    (stm/dosync
     (swap! attempts inc)
     (f))
    (let [extra (dec @attempts)]
      (when (pos? extra)
        (.add bench-aborts (long extra))))))

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

(defn- run-contention [make-ref alter-ref deref-fn dosync n-threads n-txns]
  (reset-aborts!)
  (let [counter (make-ref 0)
        [threads ^CountDownLatch gate] (launch-threads n-threads
                                                       (fn [_ _tlr]
                                                         (dotimes [_ n-txns]
                                                           (dosync #(alter-ref counter inc)))))
        [_ elapsed] (timed (.countDown gate) (join-all threads))
        total   (* n-threads n-txns)
        final   (deref-fn counter)
        commits total]
    {:threads           n-threads
     :total-txns        total
     :elapsed-ms        (format "%.2f" elapsed)
     :txns-per-sec      (format "%.0f" (/ total (/ elapsed 1000)))
     :aborts-per-commit (format "%.2f" (/ (double (abort-count)) commits))
     :final-value       final
     :expected          total
     :correct?          (= final total)}))

(defn- run-low-contention [make-ref alter-ref deref-fn dosync n-threads n-txns]
  (reset-aborts!)
  (let [refs    (vec (repeatedly n-threads #(make-ref 0)))
        [threads ^CountDownLatch gate] (launch-threads n-threads
                                                       (fn [i _tlr]
                                                         (let [r (nth refs i)]
                                                           (dotimes [_ n-txns]
                                                             (dosync #(alter-ref r inc))))))
        [_ elapsed] (timed (.countDown gate) (join-all threads))
        total   (* n-threads n-txns)
        finals  (mapv deref-fn refs)]
    {:threads           n-threads
     :total-txns        total
     :elapsed-ms        (format "%.2f" elapsed)
     :txns-per-sec      (format "%.0f" (/ total (/ elapsed 1000)))
     :aborts-per-commit (format "%.2f" (/ (double (abort-count)) total))
     :contention        :low
     :correct?          (every? #(= % n-txns) finals)}))

(defn- run-read-write-mix [make-ref alter-ref deref-fn dosync n-refs n-threads n-ops write-ratio]
  (reset-aborts!)
  (let [refs         (vec (repeatedly n-refs #(make-ref 0)))
        n-refs-int   (int n-refs)
        total-writes (LongAdder.)
        total-reads  (LongAdder.)
        [threads ^CountDownLatch gate] (launch-threads n-threads
                                                       (fn [_ ^ThreadLocalRandom tlr]
                                                         (dotimes [_ n-ops]
                                                           (if (< (.nextDouble tlr) write-ratio)
                                                             (do (.increment total-writes)
                                                                 (let [idx (.nextInt tlr n-refs-int)]
                                                                   (dosync #(alter-ref (nth refs idx) inc))))
                                                             (do (.increment total-reads)
                                                                 (dosync #(reduce + (map deref-fn refs))))))))
        [_ elapsed] (timed (.countDown gate) (join-all threads))
        total-ops   (* n-threads n-ops)
        writes      (.sum total-writes)
        reads       (.sum total-reads)
        final-sum   (reduce + (map deref-fn refs))
        ;; Aborts are only meaningful on write transactions; read-only
        ;; transactions commit without validation so they never abort.
        aborts-per-write-commit (when (pos? writes)
                                  (format "%.2f" (/ (double (abort-count)) writes)))]
    {:threads                   n-threads
     :refs                      n-refs
     :total-ops                 total-ops
     :writes                    writes
     :reads                     reads
     :write-ratio               (format "%.0f%%" (* write-ratio 100))
     :elapsed-ms                (format "%.2f" elapsed)
     :txns-per-sec              (format "%.0f" (/ total-ops (/ elapsed 1000)))
     :aborts-per-commit         aborts-per-write-commit
     :final-sum                 final-sum
     :correct?                  (= final-sum writes)}))

;; ---------------------------------------------------------------------------
;; run-scenario variants
;;
;; run-scenario        — standard path, plain dosync, no abort counting.
;; run-scenario-counted — uses counting-dosync; prints aborts/commit alongside
;;                        TPS.  Used for write-heavy and high-contention runs
;;                        where the ratio is meaningful to report.
;; ---------------------------------------------------------------------------

(defn- run-scenario [label f]
  (println (str "\n" label))
  ;; Warm-up: run once with real concurrent threads so the JIT sees the hot path.
  (f stm/ref stm/alter stm/deref #(stm/dosync (%)))
  (let [result (f stm/ref stm/alter stm/deref #(stm/dosync (%)))]
    (println (format "  opusdb  txns/sec: %s  correct: %s"
                     (:txns-per-sec result) (:correct? result)))))

(defn- run-scenario-counted [label f]
  (println (str "\n" label))
  ;; Warm-up with counting wrapper so the JIT compiles the same path.
  (f stm/ref stm/alter stm/deref counting-dosync)
  (let [result (f stm/ref stm/alter stm/deref counting-dosync)]
    (println (format "  opusdb  txns/sec: %s  aborts/commit: %s  correct: %s"
                     (:txns-per-sec result)
                     (or (:aborts-per-commit result) "n/a")
                     (:correct? result)))))

(defn run-throughput-benchmarks []
  (println "\n=== Throughput Benchmarks ===")

  (println "\n--- High Contention (single ref) ---")
  (doseq [n thread-counts]
    (run-scenario-counted (str "  " n " threads:")
                          (fn [m a d s] (run-contention m a d s n 10000))))

  (println "\n--- Low Contention (isolated refs) ---")
  (doseq [n thread-counts]
    (run-scenario (str "  " n " threads:")
                  (fn [m a d s] (run-low-contention m a d s n 10000))))

  (bank/run-bank-throughput-suite)

  (println "\n--- Read-Heavy Mix (10% writes, 10 refs) ---")
  (doseq [n thread-counts]
    (run-scenario (str "  " n " threads:")
                  (fn [m a d s] (run-read-write-mix m a d s 10 n 5000 0.1))))

  (println "\n--- Write-Heavy Mix (90% writes, 10 refs) ---")
  (doseq [n thread-counts]
    (run-scenario-counted (str "  " n " threads:")
                          (fn [m a d s] (run-read-write-mix m a d s 10 n 5000 0.9)))))

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
              #(stm/dosync (reduce + (map stm/deref rs))))]
           ["Read-only 10 refs"
            (let [rs (vec (repeatedly 10 #(stm/ref 0)))]
              #(stm/dosync (reduce + (map stm/deref rs))))]
           ["Write 5 refs"
            (let [rs (vec (repeatedly 5 #(stm/ref 0)))]
              #(stm/dosync (doseq [r rs] (stm/alter r inc))))]]]
    (println (str "\n" label " — opusdb:"))
    (crit/quick-bench (thunk))))

(defn run-all-benchmarks []
  (run-throughput-benchmarks)
  (run-criterium-benchmarks))