(ns opusdb.main
  (:require [opusdb.benchmark.report     :as report]
            [opusdb.benchmark.throughput :refer [run-all-benchmarks]]
            [opusdb.benchmark.bank       :refer [benchmark-bank]]
            [opusdb.benchmark.jfr        :refer [run-jfr-benchmark]])
  (:gen-class))

(defn -main [& args]
  (case (first args)

    "jfr"
    (do (run-jfr-benchmark (second args))
        (shutdown-agents))

    "report"
    (do (report/generate-pgf!
         (slurp (or (second args) *in*))
         "doc/figures/")
        (println "\nReport generated in doc/figures/")
        (shutdown-agents))

    (do (println "\nStarting OpusDB Performance Suite")
        (println "------------------------------------")
        (report/capture-and-generate-pgf!
         #(do (run-all-benchmarks)
              (benchmark-bank))
         "doc/figures/")
        (println "\nBenchmarking Complete.")
        (println "Reports generated in doc/figures/")
        (shutdown-agents))))