(ns opusdb.benchmark.bank
  (:require
   [criterium.core :as crit]
   [opusdb.atomic.stm :as stm]))

(defn make-bank [n-accounts initial-balance]
  {:accounts  (vec (repeatedly n-accounts #(stm/ref initial-balance)))
   :transfers (stm/ref 0)})

(defn transfer [bank from to amount]
  (stm/dosync
   (let [from-bal (stm/deref ((:accounts bank) from))
         to-bal   (stm/deref ((:accounts bank) to))]
     (when (>= from-bal amount)
       (stm/ref-set ((:accounts bank) from) (- from-bal amount))
       (stm/ref-set ((:accounts bank) to)   (+ to-bal amount))
       (stm/alter (:transfers bank) inc)
       true))))

;; Added for latency benchmarking to prevent "draining" accounts to 0
(defn- transfer-forced [bank from to amount]
  (stm/dosync
   (let [from-bal (stm/deref ((:accounts bank) from))
         to-bal   (stm/deref ((:accounts bank) to))]
     (stm/ref-set ((:accounts bank) from) (- from-bal amount))
     (stm/ref-set ((:accounts bank) to)   (+ to-bal amount))
     (stm/alter (:transfers bank) inc)
     true)))

(defn total-balance [bank]
  (stm/dosync (reduce + (map stm/deref (:accounts bank)))))

(defn benchmark-bank []
  (println "\n=== BANK TRANSFERS ===")
  (let [n-accounts 10
        bank       (make-bank n-accounts 1000)
        running    (atom true)
        threads    (mapv (fn [_]
                           (Thread.
                            ^Runnable
                            (fn []
                              (while @running
                                (let [from (rand-int n-accounts)
                                      to   (rand-int n-accounts)]
                                  (when (not= from to)
                                    (transfer bank from to
                                              (inc (rand-int 50)))))))))
                         (range 20))]
    (run! #(.start ^Thread %) threads)
    (Thread/sleep 5000)
    (reset! running false)
    (run! #(.join ^Thread % 1000) threads)
    (println "Total (should be" (* n-accounts 1000) "):" (total-balance bank))
    (println "Successful transfers:" (stm/deref (:transfers bank))))

  (println "\nBenchmarking single transfer transaction:")
  (let [bank (make-bank 10 1000)]
    (crit/quick-bench
     (transfer-forced bank 0 1 10)))

  (println "\nBenchmarking low-contention concurrent transfers:")
  (println "(Rotating through different account pairs)")
  (let [bank (make-bank 10 1000)
        n    (atom 0)]
    (crit/quick-bench
     (let [i (swap! n inc)]
       (transfer-forced bank
                        (mod i 10)
                        (mod (inc i) 10)
                        5))))

  (println "\nBenchmarking medium-contention scenario:")
  (println "(All transfers touching accounts 0-4)")
  (let [bank (make-bank 10 1000)
        n    (atom 0)]
    (crit/quick-bench
     (let [i (swap! n inc)]
       (transfer-forced bank
                        (mod i 5)
                        (mod (inc i) 5)
                        3))))

  (println "\nBenchmarking high-contention scenario:")
  (println "(All transfers between accounts 0 and 1)")
  (let [bank (make-bank 10 1000)]
    (crit/quick-bench
     (transfer-forced bank 0 1 1)))

  (println "\nBenchmarking extreme-contention with futures:")
  (println "(20 threads all transferring from/to same 2 accounts)")
  (let [bank (make-bank 10 1000)]
    (crit/quick-bench
     (let [fs (mapv (fn [_] (future (transfer-forced bank 0 1 1))) (range 20))]
       (run! deref fs)))))