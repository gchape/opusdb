(ns opusdb.main
  (:require [opusdb.benchmark.report :as report]
            [opusdb.benchmark.throughput :refer [run-all-benchmarks]]
            [opusdb.benchmark.bank :refer [benchmark-bank]])
  (:gen-class))

(defn -main [& _args]
  (println "\nStarting OpusDB Performance Suite")
  (println "------------------------------------")
  (report/capture-and-generate-pgf!
   #(do
      (run-all-benchmarks)
      (benchmark-bank))
   "doc/figures/")

  (println "\nBenchmarking Complete.")
  (println "Reports generated in doc/figures/")
  (shutdown-agents))