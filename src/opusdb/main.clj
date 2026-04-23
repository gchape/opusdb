(ns opusdb.main
  (:require [opusdb.benchmark.report :as report]
            [opusdb.benchmark.throughput :refer [run-all-benchmarks]]
            [opusdb.benchmark.bank :refer [benchmark-bank]])
  (:gen-class))

(defn -main [& _args]
  (report/capture-and-generate-pgf!
   #(do (run-all-benchmarks) (benchmark-bank))
   "doc/figures/"))