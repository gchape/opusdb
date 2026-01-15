(ns opusdb.benchmark.bank-tran
  (:require
   [criterium.core :as crit]
   [opusdb.atomic.stm :as stm]))

(defn make-bank [n-accounts initial-balance]
  {:accounts (vec (repeatedly n-accounts #(stm/ref initial-balance)))
   :transfers (stm/ref 0)})

(defn transfer [bank from to amount]
  (stm/dosync
   (let [from-bal (stm/deref ((:accounts bank) from))
         to-bal (stm/deref ((:accounts bank) to))]
     (when (>= from-bal amount)
       (stm/ref-set ((:accounts bank) from) (- from-bal amount))
       (stm/ref-set ((:accounts bank) to) (+ to-bal amount))
       (stm/alter (:transfers bank) inc)
       true))))

(defn benchmark-bank []
  (println "\n=== BANK TRANSFERS ===")
  (let [n-accounts 10
        bank (make-bank n-accounts 1000)
        duration-ms 5000
        running (atom true)
        threads (doall
                 (for [_ (range 20)]
                   (Thread.
                    (fn []
                      (while @running
                        (let [from (rand-int n-accounts)
                              to (rand-int n-accounts)]
                          (when (not= from to)
                            (transfer bank from to (inc (rand-int 50))))))))))]

    (doseq [t threads] (.start t))
    (Thread/sleep duration-ms)
    (reset! running false)
    (doseq [t threads] (.join t 1000))

    (let [total-balance (reduce + (map stm/deref (:accounts bank)))
          transfers (stm/deref (:transfers bank))]
      (println "Total (should be" (* n-accounts 1000) "):" total-balance)
      (println "Successful transfers:" transfers)))

  (println "\nBenchmarking single transfer transaction:")
  (let [bank (make-bank 10 1000)]
    (crit/bench
     (transfer bank 0 1 10)))

  (println "\nBenchmarking concurrent transfers:")
  (println "(10 concurrent transfer operations)")
  (let [bank (make-bank 10 1000)]
    (crit/bench
     (let [futures (doall
                    (for [i (range 10)]
                      (future
                        (transfer bank
                                  (mod i 10)
                                  (mod (inc i) 10)
                                  5))))]
       (doseq [f futures] @f))))

  (println "\nBenchmarking high-contention scenario:")
  (println "(20 threads all transferring from/to same 2 accounts)")
  (let [bank (make-bank 10 1000)]
    (crit/bench
     (let [futures (doall
                    (for [_ (range 20)]
                      (future
                        (transfer bank 0 1 1))))]
       (doseq [f futures] @f)))))

(benchmark-bank)