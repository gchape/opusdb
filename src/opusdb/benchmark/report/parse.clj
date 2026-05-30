(ns opusdb.benchmark.report.parse
  (:require [clojure.string :as str]))

(defn- ->double [s]
  (when (seq s)
    (try (Double/parseDouble (str/replace s #"[%,\s]" ""))
         (catch NumberFormatException _ nil))))

(defn- ->long [s]
  (when (seq s)
    (try (Long/parseLong (str/replace s #"[%,\s]" ""))
         (catch NumberFormatException _ nil))))

(defn- to-micros [value unit]
  (when value
    (case unit
      "ns"  (/ value 1000.0)
      "µs"  (double value)
      "ms"  (* value 1000.0)
      "sec" (* value 1.0e6)
      (double value))))

(def ^:private re-top-header #"(?m)^===\s+(.+?)\s+===\s*$")
(def ^:private re-sec-header #"(?m)^---\s+(.+?)\s+---\s*$")
(def ^:private re-threads    #"^\s*(\d+)\s+threads:\s*$")

(def ^:private re-impl-plain
  #"^\s*opusdb\s+txns/sec:\s+(\d+)\s+correct:\s+(true|false)")

(def ^:private re-impl-full
  #"^\s*opusdb\s+txns/sec:\s+(\d+)\s+conflict-aborts/commit:\s+([\d.]+)\s+trim-aborts/commit:\s+([\d.]+)\s+correct:\s+(true|false)")

(def ^:private re-crit-label  #"^(.+?)\s*—\s*opusdb:\s*$")
(def ^:private re-crit-mean   #"Execution time mean\s*[:]?\s*([\d.]+)\s*(ns|µs|ms|sec)")
(def ^:private re-crit-std    #"Execution time std-deviation\s*[:]?\s*([\d.]+)\s*(ns|µs|ms|sec)")
(def ^:private re-crit-lower  #"Execution time lower quantile\s*[:]?\s*([\d.]+)\s*(ns|µs|ms|sec)")
(def ^:private re-crit-upper  #"Execution time upper quantile\s*[:]?\s*([\d.]+)\s*(ns|µs|ms|sec)")

(def ^:private re-bank-total    #"Total\s+\(should be\s+(\d+)\s*\):\s*(\d+)")
(def ^:private re-bank-xfers    #"Successful transfers:\s*(\d+)")
(def ^:private re-bank-scenario #"^Benchmarking\s+(.+?):")

(def ^:private re-history-sweep
  #"^\s*(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s*$")

(defn- parse-throughput-section [lines]
  (loop [[line & rest] lines
         cur-threads nil
         cur-vals    {}
         acc         []]
    (cond
      (nil? line)
      (cond-> acc
        (and cur-threads (seq cur-vals))
        (conj (assoc cur-vals :threads cur-threads)))

      (re-find re-threads line)
      (let [n    (->long (second (re-find re-threads line)))
            acc' (cond-> acc
                   (and cur-threads (seq cur-vals))
                   (conj (assoc cur-vals :threads cur-threads)))]
        (recur rest n {} acc'))

      ;; Full line (conflict + trim aborts) — tested before plain
      (re-find re-impl-full line)
      (let [[_ tps ca ta correct] (re-find re-impl-full line)]
        (recur rest cur-threads
               (assoc cur-vals
                      :opusdb                     (->long tps)
                      :conflict-aborts-per-commit (->double ca)
                      :trim-aborts-per-commit     (->double ta)
                      :correct?                   (= correct "true"))
               acc))

      (re-find re-impl-plain line)
      (let [[_ tps correct] (re-find re-impl-plain line)]
        (recur rest cur-threads
               (assoc cur-vals
                      :opusdb   (->long tps)
                      :correct? (= correct "true"))
               acc))

      :else
      (recur rest cur-threads cur-vals acc))))

(defn- parse-history-sweep [lines]
  (reduce
   (fn [acc line]
     (if-let [[_ h n tps ca ta] (re-find re-history-sweep line)]
       (conj acc {:max-history               (->long h)
                  :threads                   (->long n)
                  :txns-per-sec              (->long tps)
                  :conflict-aborts-per-commit (->double ca)
                  :trim-aborts-per-commit     (->double ta)})
       acc))
   []
   lines))

(defn- extract-time [re line]
  (when-let [[_ v u] (re-find re line)]
    (to-micros (->double v) u)))

(defn- merge-timing [data line]
  (reduce
   (fn [d [k re]]
     (if-let [v (extract-time re line)]
       (assoc d k v)
       d))
   data
   [[:mean  re-crit-mean]
    [:std   re-crit-std]
    [:lower re-crit-lower]
    [:upper re-crit-upper]]))

(defn- parse-criterium-blocks [lines]
  (loop [[line & rest] lines
         cur-label nil
         cur-data  {}
         acc       {}]
    (cond
      (nil? line)
      (cond-> acc
        (and cur-label (seq cur-data))
        (assoc cur-label cur-data))

      (re-find re-crit-label line)
      (let [[_ label] (re-find re-crit-label line)
            acc'      (cond-> acc
                        (and cur-label (seq cur-data))
                        (assoc cur-label cur-data))]
        (recur rest label {} acc'))

      :else
      (recur rest cur-label (merge-timing cur-data line) acc))))

(defn- parse-bank-blocks [lines]
  (loop [[line & rest] lines
         cur-scenario  nil
         cur-data      {}
         acc           {:integrity nil :total-transfers nil :scenarios []}]
    (cond
      (nil? line)
      (cond-> acc
        (and cur-scenario (:mean cur-data))
        (update :scenarios conj (assoc cur-data :label cur-scenario)))

      (re-find re-bank-total line)
      (let [[_ expected actual] (re-find re-bank-total line)]
        (recur rest cur-scenario cur-data
               (assoc acc :integrity {:expected (->long expected)
                                      :actual   (->long actual)
                                      :correct? (= expected actual)})))

      (re-find re-bank-xfers line)
      (let [[_ n] (re-find re-bank-xfers line)]
        (recur rest cur-scenario cur-data
               (assoc acc :total-transfers (->long n))))

      (re-find re-bank-scenario line)
      (let [[_ label] (re-find re-bank-scenario line)
            acc'      (cond-> acc
                        (and cur-scenario (:mean cur-data))
                        (update :scenarios conj (assoc cur-data :label cur-scenario)))]
        (recur rest label {} acc'))

      :else
      (recur rest cur-scenario
             (-> cur-data
                 (as-> d (if-let [v (extract-time re-crit-mean line)] (assoc d :mean v) d))
                 (as-> d (if-let [v (extract-time re-crit-std  line)] (assoc d :std  v) d)))
             acc))))

(defn- split-sections [lines]
  (loop [[line & rest] lines
         cur-top   nil
         cur-sec   nil
         cur-lines []
         acc       {}]
    (cond
      (nil? line)
      (cond-> acc cur-sec (assoc-in [cur-top cur-sec] cur-lines))

      (re-find re-top-header line)
      (let [[_ label] (re-find re-top-header line)
            acc'      (cond-> acc cur-sec (assoc-in [cur-top cur-sec] cur-lines))]
        (recur rest (str/trim label) nil [] acc'))

      (re-find re-sec-header line)
      (let [[_ label] (re-find re-sec-header line)
            acc'      (cond-> acc cur-sec (assoc-in [cur-top cur-sec] cur-lines))]
        (recur rest cur-top (str/trim label) [] acc'))

      :else
      (recur rest cur-top cur-sec (conj cur-lines line) acc))))

(defn parse-all [^String stdout]
  (let [lines    (str/split-lines stdout)
        sections (split-sections lines)]
    {:throughput    (some-> (get sections "Throughput Benchmarks")
                            (dissoc "Version History Size Sweep (bank transfer, 20 accounts)")
                            (update-vals parse-throughput-section))
     :history-sweep (some-> (get-in sections ["Throughput Benchmarks"
                                              "Version History Size Sweep (bank transfer, 20 accounts)"])
                            parse-history-sweep)
     :criterium     (some-> (get sections "Criterium Statistical Benchmarks")
                            vals
                            (->> (apply concat))
                            vec
                            parse-criterium-blocks)
     :bank          (parse-bank-blocks lines)}))