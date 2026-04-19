(ns opusdb.benchmark.throughput
  (:require
   [criterium.core :as crit]
   [opusdb.atomic.stm :as stm])
  (:import
   [java.util.concurrent CountDownLatch]))

(defmacro ^:private timed [& body]
  `(let [start# (System/nanoTime)
         result# (do ~@body)]
     [result# (/ (- (System/nanoTime) start#) 1e6)]))

(defn- launch-threads [n f]
  ;; ready latch synchronises all threads to start simultaneously
  (let [ready   (CountDownLatch. n)
        gate    (CountDownLatch. 1)
        threads (into-array Thread
                            (for [i (range n)]
                              (Thread.
                               ^Runnable
                               (fn []
                                 (.countDown ready)
                                 (.await ^CountDownLatch gate)
                                 (f i)))))]
    (doseq [^Thread t threads] (.start t))
    (.await ready)
    [threads gate]))

(defn- join-all [threads]
  (doseq [^Thread t threads] (.join t)))

(defn- run-contention [make-ref alter-ref deref-fn dosync n-threads n-txns]
  (let [counter (make-ref 0)
        [threads ^CountDownLatch gate] (launch-threads n-threads
                                                       (fn [_]
                                                         (dotimes [_ n-txns]
                                                           (dosync #(alter-ref counter inc)))))
        [_ elapsed] (timed (.countDown gate) (join-all threads))
        total   (* n-threads n-txns)
        final   (deref-fn counter)]
    {:threads      n-threads
     :total-txns   total
     :elapsed-ms   (format "%.2f" elapsed)
     :txns-per-sec (format "%.0f" (/ total (/ elapsed 1000)))
     :final-value  final
     :expected     total
     :correct?     (= final total)}))

(defn- run-low-contention [make-ref alter-ref deref-fn dosync n-threads n-txns]
  ;; each thread owns a dedicated ref so there is no write conflict
  (let [refs    (vec (repeatedly n-threads #(make-ref 0)))
        [threads ^CountDownLatch gate] (launch-threads n-threads
                                                       (fn [i]
                                                         (let [r (nth refs i)]
                                                           (dotimes [_ n-txns]
                                                             (dosync #(alter-ref r inc))))))
        [_ elapsed] (timed (.countDown gate) (join-all threads))
        total   (* n-threads n-txns)
        finals  (mapv deref-fn refs)]
    {:threads      n-threads
     :total-txns   total
     :elapsed-ms   (format "%.2f" elapsed)
     :txns-per-sec (format "%.0f" (/ total (/ elapsed 1000)))
     :contention   :low
     :correct?     (every? #(= % n-txns) finals)}))

(defn- run-bank-transfer [make-ref alter-ref deref-fn dosync n-accounts n-threads n-txns]
  (let [accounts   (vec (repeatedly n-accounts #(make-ref 1000)))
        successful (atom 0)
        [threads ^CountDownLatch gate] (launch-threads n-threads
                                                       (fn [_]
                                                         (dotimes [_ n-txns]
                                                           (let [from-idx (rand-int n-accounts)
                                                                 to-idx   (rand-int n-accounts)]
                                                             (when (not= from-idx to-idx)
                                                               (let [from (nth accounts from-idx)
                                                                     to   (nth accounts to-idx)
                                                                     amt  (inc (rand-int 100))]
                                                                 (when (dosync
                                                                        #(when (>= (deref-fn from) amt)
                                                                           (alter-ref from - amt)
                                                                           (alter-ref to + amt)
                                                                           true))
                                                                   (swap! successful inc))))))))
        [_ elapsed] (timed (.countDown gate) (join-all threads))
        succ      @successful
        total-bal (dosync #(reduce + (map deref-fn accounts)))]
    {:threads          n-threads
     :accounts         n-accounts
     :attempted-txns   (* n-threads n-txns)
     :successful-txns  succ
     :elapsed-ms       (format "%.2f" elapsed)
     :txns-per-sec     (format "%.0f" (/ succ (/ elapsed 1000)))
     :total-balance    total-bal
     :expected-balance (* n-accounts 1000)
     :correct?         (= total-bal (* n-accounts 1000))}))

(defn- run-read-write-mix [make-ref alter-ref deref-fn dosync n-refs n-threads n-ops write-ratio]
  (let [refs         (vec (repeatedly n-refs #(make-ref 0)))
        total-writes (atom 0)
        [threads ^CountDownLatch gate] (launch-threads n-threads
                                                       (fn [_]
                                                         (dotimes [_ n-ops]
                                                           (if (< (rand) write-ratio)
                                                             (do (swap! total-writes inc)
                                                                 (dosync #(alter-ref (rand-nth refs) inc)))
                                                             (dosync #(reduce + (map deref-fn refs)))))))
        [_ elapsed] (timed (.countDown gate) (join-all threads))
        total-ops   (* n-threads n-ops)
        writes      @total-writes
        final-sum   (reduce + (map deref-fn refs))]
    {:threads      n-threads
     :refs         n-refs
     :total-ops    total-ops
     :writes       writes
     :reads        (- total-ops writes)
     :write-ratio  (format "%.0f%%" (* write-ratio 100))
     :elapsed-ms   (format "%.2f" elapsed)
     :txns-per-sec (format "%.0f" (/ total-ops (/ elapsed 1000)))
     :final-sum    final-sum
     :correct?     (= final-sum writes)}))

(defn- compare-scenario [label f]
  (println (str "\n" label))
  (let [opus   (f stm/ref stm/alter stm/deref
                  #(stm/dosync (%)))
        native (f clojure.core/ref clojure.core/alter clojure.core/deref
                  #(clojure.core/dosync (%)))]
    (println (format "  %-12s txns/sec: %s  correct: %s"
                     "opusdb" (:txns-per-sec opus)   (:correct? opus)))
    (println (format "  %-12s txns/sec: %s  correct: %s"
                     "native" (:txns-per-sec native) (:correct? native)))))

(defn- run-throughput-benchmarks []
  (println "\n=== Throughput Benchmarks ===")

  (println "\n--- High Contention (single ref) ---")
  (doseq [n [4 8 16]]
    (compare-scenario (str "  " n " threads:")
                      (fn [make-ref alter-ref deref-fn dosync]
                        (run-contention make-ref alter-ref deref-fn dosync n 10000))))

  (println "\n--- Low Contention (isolated refs) ---")
  (doseq [n [4 8 16]]
    (compare-scenario (str "  " n " threads:")
                      (fn [make-ref alter-ref deref-fn dosync]
                        (run-low-contention make-ref alter-ref deref-fn dosync n 10000))))

  (println "\n--- Bank Transfer (20 accounts) ---")
  (doseq [n [4 8 16]]
    (compare-scenario (str "  " n " threads:")
                      (fn [make-ref alter-ref deref-fn dosync]
                        (run-bank-transfer make-ref alter-ref deref-fn dosync 20 n 5000))))

  (println "\n--- Read-Heavy Mix (10% writes, 10 refs) ---")
  (doseq [n [4 8]]
    (compare-scenario (str "  " n " threads:")
                      (fn [make-ref alter-ref deref-fn dosync]
                        (run-read-write-mix make-ref alter-ref deref-fn dosync 10 n 5000 0.1))))

  (println "\n--- Write-Heavy Mix (90% writes, 10 refs) ---")
  (doseq [n [4 8]]
    (compare-scenario (str "  " n " threads:")
                      (fn [make-ref alter-ref deref-fn dosync]
                        (run-read-write-mix make-ref alter-ref deref-fn dosync 10 n 5000 0.9)))))

(defn- run-criterium-benchmarks []
  (println "\n=== Criterium Statistical Benchmarks ===")
  (doseq [[label opus-thunk native-thunk]
          [["Single increment"
            (let [r (stm/ref 0)]
              #(stm/dosync (stm/alter r inc)))
            (let [r (clojure.core/ref 0)]
              #(clojure.core/dosync (clojure.core/alter r inc)))]

           ["Single ref-set"
            (let [r (stm/ref 0)]
              #(stm/dosync (stm/ref-set r 42)))
            (let [r (clojure.core/ref 0)]
              #(clojure.core/dosync (ref-set r 42)))]

           ["Read-only 5 refs"
            (let [rs (vec (repeatedly 5 #(stm/ref 0)))]
              #(stm/dosync (reduce + (map stm/deref rs))))
            (let [rs (vec (repeatedly 5 #(clojure.core/ref 0)))]
              #(clojure.core/dosync (reduce + (map clojure.core/deref rs))))]

           ["Read-only 10 refs"
            (let [rs (vec (repeatedly 10 #(stm/ref 0)))]
              #(stm/dosync (reduce + (map stm/deref rs))))
            (let [rs (vec (repeatedly 10 #(clojure.core/ref 0)))]
              #(clojure.core/dosync (reduce + (map clojure.core/deref rs))))]

           ["Write 5 refs"
            (let [rs (vec (repeatedly 5 #(stm/ref 0)))]
              #(stm/dosync (doseq [r rs] (stm/alter r inc))))
            (let [rs (vec (repeatedly 5 #(clojure.core/ref 0)))]
              #(clojure.core/dosync (doseq [r rs] (clojure.core/alter r inc))))]]]

    (println (str "\n" label " — opusdb:"))
    (crit/quick-bench (opus-thunk))
    (println (str "\n" label " — native:"))
    (crit/quick-bench (native-thunk))))

(defn run-all-benchmarks []
  (run-throughput-benchmarks)
  (run-criterium-benchmarks))

(run-all-benchmarks)