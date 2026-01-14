(ns opusdb.benchmark.santa-claus
  (:require
   [criterium.core :as crit]
   [opusdb.atomic.stm :as stm]))

;; Santa Claus Problem:
;; - 9 reindeer return from vacation (all must be back to deliver)
;; - 10 elves need help (groups of 3 at a time)
;; - Reindeer have priority over elves

(def NUM_REINDEER 9)
(def NUM_ELVES 10)
(def GROUP_SIZE 3)

(defn make-state []
  {:reindeer-back (stm/ref #{})
   :elves-waiting (stm/ref [])
   :deliveries (stm/ref 0)
   :elf-meetings (stm/ref 0)})

(defn reindeer-return [state id]
  (stm/dosync
   (stm/alter (:reindeer-back state) conj id)
   (when (= (count (stm/deref (:reindeer-back state))) NUM_REINDEER)
     (stm/ref-set (:reindeer-back state) #{})
     (stm/alter (:deliveries state) inc)
     :delivered)))

(defn elf-request-help [state id]
  (stm/dosync
   (let [reindeer-count (count (stm/deref (:reindeer-back state)))
         elves (stm/deref (:elves-waiting state))]
     (when (< reindeer-count NUM_REINDEER)  ; Don't interrupt reindeer
       (let [elves' (conj elves id)]
         (stm/ref-set (:elves-waiting state) elves')
         (when (>= (count elves') GROUP_SIZE)
           (stm/ref-set (:elves-waiting state) (vec (drop GROUP_SIZE elves')))
           (stm/alter (:elf-meetings state) inc)
           :helped))))))

(defn run-simulation [duration-ms]
  (let [state (make-state)
        running (atom true)

        reindeer-threads
        (doall
         (for [id (range NUM_REINDEER)]
           (Thread.
            (fn []
              (while @running
                (Thread/sleep (+ 100 (rand-int 200)))
                (reindeer-return state id))))))

        elf-threads
        (doall
         (for [id (range NUM_ELVES)]
           (Thread.
            (fn []
              (while @running
                (Thread/sleep (+ 50 (rand-int 100)))
                (elf-request-help state id))))))]

    (doseq [t (concat reindeer-threads elf-threads)]
      (.start t))

    (Thread/sleep duration-ms)
    (reset! running false)

    (doseq [t (concat reindeer-threads elf-threads)]
      (.join t 1000))

    {:deliveries (stm/deref (:deliveries state))
     :elf-meetings (stm/deref (:elf-meetings state))
     :reindeer-waiting (count (stm/deref (:reindeer-back state)))
     :elves-waiting (count (stm/deref (:elves-waiting state)))}))

(defn benchmark-santa-claus []
  (println "\n=== Santa Claus Problem Benchmark ===\n")

  (println "Running 10-second simulation...")
  (println "(9 reindeer threads + 10 elf threads)\n")
  (let [result (run-simulation 10000)]
    (println "Results:")
    (println "  Toy deliveries:" (:deliveries result))
    (println "  Elf meetings:" (:elf-meetings result))
    (println "  Reindeer still waiting:" (:reindeer-waiting result))
    (println "  Elves still waiting:" (:elves-waiting result)))

  (println "\n\nBenchmarking concurrent transactions:")
  (let [state (make-state)]
    (crit/quick-bench
     (let [futures (doall
                    (for [i (range 19)]
                      (future
                        (if (< i 9)
                          (reindeer-return state i)
                          (elf-request-help state (- i 9))))))]
       (doseq [f futures] @f)))))

(benchmark-santa-claus)