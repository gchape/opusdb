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

(def ^:private re-top-header    #"(?m)^=== (.+?) ===\s*$")
(def ^:private re-sec-header    #"(?m)^--- (.+?) ---\s*$")
(def ^:private re-threads       #"^\s*(\d+) threads:\s*$")
(def ^:private re-impl-line     #"^\s*opusdb\s+txns/sec:\s+(\d+)\s+correct:\s+(true|false)")
(def ^:private re-crit-label    #"^(.+?) — opusdb:\s*$")
(def ^:private re-crit-mean     #"Execution time mean\s*:\s*([\d.]+)\s*(ns|µs|ms|sec)")
(def ^:private re-crit-std      #"Execution time std-deviation\s*:\s*([\d.]+)\s*(ns|µs|ms|sec)")
(def ^:private re-crit-lower    #"Execution time lower quantile\s*:\s*([\d.]+)\s*(ns|µs|ms|sec)")
(def ^:private re-crit-upper    #"Execution time upper quantile\s*:\s*([\d.]+)\s*(ns|µs|ms|sec)")
(def ^:private re-bank-total    #"Total \(should be (\d+) \):\s*(\d+)")
(def ^:private re-bank-xfers    #"Successful transfers:\s*(\d+)")
(def ^:private re-bank-scenario #"Benchmarking (.+?):")

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

      (re-find re-impl-line line)
      (let [[_ tps correct] (re-find re-impl-line line)]
        (recur rest cur-threads
               (assoc cur-vals
                      :opusdb (->long tps)
                      :correct? (= correct "true"))
               acc))

      :else
      (recur rest cur-threads cur-vals acc))))

(defn- extract-time [re line]
  (when-let [[_ v u] (re-find re line)]
    (to-micros (->double v) u)))

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
      (let [cur-data' (cond-> cur-data
                        (extract-time re-crit-mean line)
                        (assoc :mean  (extract-time re-crit-mean line))
                        (extract-time re-crit-std line)
                        (assoc :std   (extract-time re-crit-std line))
                        (extract-time re-crit-lower line)
                        (assoc :lower (extract-time re-crit-lower line))
                        (extract-time re-crit-upper line)
                        (assoc :upper (extract-time re-crit-upper line)))]
        (recur rest cur-label cur-data' acc)))))

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
      (let [cur-data' (cond-> cur-data
                        (extract-time re-crit-mean line)
                        (assoc :mean (extract-time re-crit-mean line))
                        (extract-time re-crit-std line)
                        (assoc :std  (extract-time re-crit-std line)))]
        (recur rest cur-scenario cur-data' acc)))))

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
        (recur rest label nil [] acc'))

      (re-find re-sec-header line)
      (let [[_ label] (re-find re-sec-header line)
            acc'      (cond-> acc cur-sec (assoc-in [cur-top cur-sec] cur-lines))]
        (recur rest cur-top label [] acc'))

      :else
      (recur rest cur-top cur-sec (conj cur-lines line) acc))))

(defn parse-throughput [^String stdout]
  (let [lines    (str/split-lines stdout)
        sections (split-sections lines)]
    {:throughput (some-> (get sections "Throughput Benchmarks")
                         (update-vals parse-throughput-section))
     :criterium  (some-> (get sections "Criterium Statistical Benchmarks")
                         vals
                         (->> (apply concat))
                         vec
                         parse-criterium-blocks)}))

(defn parse-bank [^String stdout]
  (parse-bank-blocks (str/split-lines stdout)))

(defn parse-all [^String stdout]
  (merge (parse-throughput stdout)
         {:bank (parse-bank stdout)}))