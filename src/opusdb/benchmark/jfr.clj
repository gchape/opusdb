(ns opusdb.benchmark.jfr
  "Dedicated JFR benchmark for commit-lock contention analysis.
   Run as a separate JVM invocation per thread count so each gets
   its own clean .jfr recording.

   Usage:
     lein with-profile bench,jfr run jfr 4
     mv stm-bench.jfr stm-bench-4t.jfr

     lein with-profile bench,jfr run jfr 6
     mv stm-bench.jfr stm-bench-6t.jfr

     lein with-profile bench,jfr run jfr 12
     mv stm-bench.jfr stm-bench-12t.jfr

   Then open each file in JDK Mission Control:
     jmc
   Navigate to: Event Browser -> Java Application -> Monitor Blocked
   Filter by class name containing 'commit-lock' or your STM namespace.
   Record: Count, Mean Duration, Max Duration per thread count."
  (:require [opusdb.benchmark.bank :as bank])
  (:import [java.util.concurrent CountDownLatch ThreadLocalRandom]
           [java.util.concurrent.atomic LongAdder]))

;; ---------------------------------------------------------------------------
;; Wall-time-driven throughput run — no Criterium, no fixed txn count.
;; Runs long enough (30s) for JFR to accumulate meaningful monitor events.
;; ---------------------------------------------------------------------------

(defn- run-jfr-bank [n-threads n-accounts duration-ms]
  (let [b      (bank/make-bank n-accounts 100000)
        stop?  (volatile! false)
        done   (LongAdder.)
        ready  (CountDownLatch. n-threads)
        gate   (CountDownLatch. 1)
        threads (into-array
                 Thread
                 (for [_ (range n-threads)]
                   (Thread.
                    ^Runnable
                    (fn []
                      (let [tlr (ThreadLocalRandom/current)
                            n   n-accounts]
                        (.countDown ready)
                        (.await ^CountDownLatch gate)
                        (while (not @stop?)
                          (let [from (.nextInt tlr n)
                                to   (int (mod (+ from 1
                                                  (.nextInt tlr (dec n)))
                                               n))
                                amt  (inc (.nextInt tlr 100))]
                            (when (bank/transfer b from to amt)
                              (.increment done)))))))))]
    (doseq [^Thread t threads] (.start t))
    (.await ready)
    (.countDown gate)
    (Thread/sleep ^long duration-ms)
    (vreset! stop? true)
    (doseq [^Thread t threads] (.join t))
    {:threads      n-threads
     :completed    (.sum done)
     :correct?     (= (bank/total-balance b) (* n-accounts 100000))
     :txns-per-sec (long (/ (.sum done) (/ duration-ms 1000)))}))

(defn run-jfr-benchmark
  "Entry point. Pass thread count as a string argument, e.g. \"4\", \"6\", \"12\"."
  [n-threads-str]
  (let [n (Integer/parseInt n-threads-str)]
    (println (format "\n[JFR] Bank Transfer — %d threads" n))

    ;; Warm up the JIT before JFR starts capturing.
    ;; 10s is enough for Clojure's hot path to be fully compiled.
    (println "  Warming JIT (10s)...")
    (run-jfr-bank n 20 10000)
    (println "  JIT warm.")

    ;; The actual measured run. 30s gives JFR enough monitor events
    ;; to produce statistically meaningful contention data.
    (println "  Running measured window (30s)...")
    (let [{:keys [completed correct? txns-per-sec]} (run-jfr-bank n 20 30000)]
      (println (format "  completed: %d  tps: %d  correct: %s"
                       completed txns-per-sec (str correct?))))

    (println (format "\n[JFR] Done. Rename stm-bench.jfr -> stm-bench-%dt.jfr" n))))