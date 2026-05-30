(ns opusdb.benchmark.report
  (:require
   [clojure.java.io :as io]
   [clojure.string  :as str]
   [cheshire.core   :as json]
   [opusdb.benchmark.report.parse :as parse]))

(defn- tex-safe [s]
  (-> (str s)
      (str/replace #"_" "\\\\_")
      (str/replace #"%" "\\\\%")
      (str/replace #"&" "\\\\&")
      (str/replace #"\$" "\\\\$")
      (str/replace #"\{" "\\\\{")
      (str/replace #"\}" "\\\\}")))

(def ^:private throughput-charts
  [["High Contention (single ref)"
    "throughput-high-contention"
    "Throughput: High Contention (single shared ref)"]
   ["Low Contention (isolated refs)"
    "throughput-low-contention"
    "Throughput: Low Contention (isolated refs)"]
   ["Bank Transfer (20 accounts)"
    "throughput-bank-transfer"
    "Throughput: Bank Transfer (20 accounts)"]
   ["Read-Heavy Mix (10% writes, 10 refs)"
    "throughput-read-heavy"
    "Throughput: Read-Heavy Mix (10\\% writes)"]
   ["Write-Heavy Mix (90% writes, 10 refs)"
    "throughput-write-heavy"
    "Throughput: Write-Heavy Mix (90\\% writes)"]])

(defn- has-conflict-aborts? [rows]
  (some :conflict-aborts-per-commit rows))

(defn- has-trim-aborts? [rows]
  (some :trim-aborts-per-commit rows))

(defn- fmt-abort [v]
  (if v (format "%.2f" (double v)) "---"))

(defn- throughput-table-row [row show-conflict? show-trim?]
  (let [tps (or (:opusdb row) 0)
        ok? (:correct? row)]
    (cond
      (and show-conflict? show-trim?)
      (format "    %d & %s & %s & %s & %s \\\\\n"
              (:threads row)
              (tex-safe (format "%,d" (long tps)))
              (tex-safe (fmt-abort (:conflict-aborts-per-commit row)))
              (tex-safe (fmt-abort (:trim-aborts-per-commit row)))
              (if ok? "\\checkmark" "\\texttimes"))

      show-conflict?
      (format "    %d & %s & %s & %s \\\\\n"
              (:threads row)
              (tex-safe (format "%,d" (long tps)))
              (tex-safe (fmt-abort (:conflict-aborts-per-commit row)))
              (if ok? "\\checkmark" "\\texttimes"))

      :else
      (format "    %d & %s & %s \\\\\n"
              (:threads row)
              (tex-safe (format "%,d" (long tps)))
              (if ok? "\\checkmark" "\\texttimes")))))

(defn- throughput-section-table [scenario-label rows]
  (let [show-conflict? (has-conflict-aborts? rows)
        show-trim?     (has-trim-aborts? rows)
        [col-spec header]
        (cond
          (and show-conflict? show-trim?)
          ["rrrrr"
           "    Threads & TPS & Conflict aborts/commit & Trim aborts/commit & Correct \\\\\n"]
          show-conflict?
          ["rrrr"
           "    Threads & TPS & Conflict aborts/commit & Correct \\\\\n"]
          :else
          ["rrr"
           "    Threads & TPS & Correct \\\\\n"])]
    (str
     "\\begin{table}[htbp]\n"
     "  \\centering\n"
     "  \\caption{" (tex-safe scenario-label) "}\n"
     "  \\begin{tabular}{@{}" col-spec "@{}}\n"
     "    \\toprule\n"
     header
     "    \\midrule\n"
     (str/join (map #(throughput-table-row % show-conflict? show-trim?) rows))
     "    \\bottomrule\n"
     "  \\end{tabular}\n"
     "\\end{table}\n")))

(defn- pgf-throughput-chart [section-data title label]
  (str
   "\\begin{figure}[htbp]\n"
   "  \\centering\n"
   "  \\begin{tikzpicture}\n"
   "    \\begin{axis}[\n"
   "      title={" (tex-safe title) "},\n"
   "      xlabel={Thread count}, ylabel={Transactions / sec},\n"
   "      ybar, bar width=18pt, width=0.75\\textwidth, height=7cm,\n"
   "      xtick=data, ymin=0, enlarge y limits={upper, value=0.15},\n"
   "      ymajorgrids=true, grid style=dashed,\n"
   "      nodes near coords, nodes near coords align={vertical},\n"
   "      every node near coord/.append style={font=\\tiny, anchor=south},\n"
   "      tick label style={font=\\small}, label style={font=\\small},\n"
   "      title style={font=\\small\\itshape}, clip=false,\n"
   "    ]\n"
   "    \\addplot[fill=blue!40!black, draw=blue!40!black] coordinates {\n"
   (str/join "\n"
             (map (fn [{:keys [threads opusdb]}]
                    (format "      (%d,%d)" threads (or opusdb 0)))
                  section-data))
   "\n    };\n"
   "    \\end{axis}\n"
   "  \\end{tikzpicture}\n"
   "  \\caption{" (tex-safe title) "}\n"
   "  \\label{fig:" label "}\n"
   "\\end{figure}\n"))

(defn- pgf-scalability-chart [section-data title label]
  (str
   "\\begin{figure}[htbp]\n"
   "  \\centering\n"
   "  \\begin{tikzpicture}\n"
   "    \\begin{axis}[\n"
   "      title={" (tex-safe title) "},\n"
   "      xlabel={Thread count},\n"
   "      ylabel={Transactions / sec},\n"
   "      width=0.75\\textwidth, height=6cm,\n"
   "      xtick=data, ymin=0,\n"
   "      ymajorgrids=true, grid style=dashed,\n"
   "      mark=*, tick label style={font=\\small},\n"
   "      label style={font=\\small}, title style={font=\\small\\itshape},\n"
   "    ]\n"
   "    \\addplot[color=blue!40!black, mark=*] coordinates {\n"
   (str/join "\n"
             (map (fn [{:keys [threads opusdb]}]
                    (format "      (%d,%d)" threads (or opusdb 0)))
                  section-data))
   "\n    };\n"
   "    \\legend{opusdb STM}\n"
   "    \\end{axis}\n"
   "  \\end{tikzpicture}\n"
   "  \\caption{" (tex-safe title) " (Scaling)}\n"
   "  \\label{fig:" label "-scaling}\n"
   "\\end{figure}\n"))

(defn- pgf-abort-charts [section-data title label]
  (when (or (has-conflict-aborts? section-data)
            (has-trim-aborts? section-data))
    (str
     "\\begin{figure}[htbp]\n"
     "  \\centering\n"
     "  \\begin{tikzpicture}\n"
     "    \\begin{axis}[\n"
     "      title={" (tex-safe (str title " — Aborts per Commit")) "},\n"
     "      xlabel={Thread count},\n"
     "      ylabel={Aborts / committed txn},\n"
     "      width=0.75\\textwidth, height=5cm,\n"
     "      xtick=data, ymin=0,\n"
     "      ymajorgrids=true, grid style=dashed,\n"
     "      mark=*, tick label style={font=\\small},\n"
     "      label style={font=\\small}, title style={font=\\small\\itshape},\n"
     "      legend pos=north west,\n"
     "    ]\n"
     "    \\addplot[color=red!60!black, mark=*] coordinates {\n"
     (str/join "\n"
               (map (fn [{:keys [threads conflict-aborts-per-commit]}]
                      (format "      (%d,%.2f)" threads (or conflict-aborts-per-commit 0.0)))
                    section-data))
     "\n    };\n"
     "    \\addlegendentry{conflict aborts}\n"
     "    \\addplot[color=orange!80!black, mark=square*] coordinates {\n"
     (str/join "\n"
               (map (fn [{:keys [threads trim-aborts-per-commit]}]
                      (format "      (%d,%.2f)" threads (or trim-aborts-per-commit 0.0)))
                    section-data))
     "\n    };\n"
     "    \\addlegendentry{trim aborts}\n"
     "    \\end{axis}\n"
     "  \\end{tikzpicture}\n"
     "  \\caption{" (tex-safe (str title " — Aborts per Commit")) "}\n"
     "  \\label{fig:" label "-aborts}\n"
     "\\end{figure}\n")))

(defn- history-sweep-table [sweep-data]
  (when (seq sweep-data)
    (str
     "\\begin{table}[htbp]\n"
     "  \\centering\n"
     "  \\caption{Version history size sweep: bank transfer throughput and abort rates}\n"
     "  \\label{tab:history-sweep}\n"
     "  \\begin{tabular}{@{}rrrrrr@{}}\n"
     "    \\toprule\n"
     "    \\texttt{max-history} & Threads & TPS & Conflict aborts/commit & Trim aborts/commit \\\\\n"
     "    \\midrule\n"
     (str/join
      (map (fn [{:keys [max-history threads txns-per-sec
                        conflict-aborts-per-commit trim-aborts-per-commit]}]
             (format "    %d & %d & %s & %s & %s \\\\\n"
                     max-history
                     threads
                     (tex-safe (format "%,d" (long (or txns-per-sec 0))))
                     (tex-safe (fmt-abort conflict-aborts-per-commit))
                     (tex-safe (fmt-abort trim-aborts-per-commit))))
           (sort-by (juxt :max-history :threads) sweep-data)))
     "    \\bottomrule\n"
     "  \\end{tabular}\n"
     "\\end{table}\n")))

(defn- history-sweep-chart [sweep-data]
  (when (seq sweep-data)
    (let [thread-counts (->> sweep-data (map :threads) distinct sort)
          colors        ["blue!60!black" "red!60!black" "green!50!black"]]
      (str
       "\\begin{figure}[htbp]\n"
       "  \\centering\n"
       "  \\begin{tikzpicture}\n"
       "    \\begin{axis}[\n"
       "      title={Throughput vs.\\ version history size},\n"
       "      xlabel={\\texttt{max-history}},\n"
       "      ylabel={Transactions / sec},\n"
       "      width=0.75\\textwidth, height=6cm,\n"
       "      xtick=data, ymin=0,\n"
       "      ymajorgrids=true, grid style=dashed,\n"
       "      mark=*, tick label style={font=\\small},\n"
       "      label style={font=\\small}, title style={font=\\small\\itshape},\n"
       "      legend pos=south east,\n"
       "    ]\n"
       (str/join
        (map-indexed
         (fn [i n]
           (let [rows (->> sweep-data
                           (filter #(= (:threads %) n))
                           (sort-by :max-history))
                 color (nth colors i "gray")]
             (str
              "    \\addplot[color=" color ", mark=*] coordinates {\n"
              (str/join "\n"
                        (map (fn [{:keys [max-history txns-per-sec]}]
                               (format "      (%d,%d)" max-history (or txns-per-sec 0)))
                             rows))
              "\n    };\n"
              "    \\addlegendentry{" n " threads}\n")))
         thread-counts))
       "    \\end{axis}\n"
       "  \\end{tikzpicture}\n"
       "  \\caption{Throughput across version history sizes (bank transfer, 20 accounts).}\n"
       "  \\label{fig:history-sweep}\n"
       "\\end{figure}\n"))))

(defn- pgf-bank-latency-chart [scenarios title label]
  (let [n (count scenarios)
        base-height-cm    3
        per-scenario-cm   0.8
        chart-height-cm   (+ base-height-cm (* n per-scenario-cm))]
    (str
     "\\begin{figure}[htbp]\n"
     "  \\centering\n"
     "  \\begin{tikzpicture}\n"
     "    \\begin{axis}[\n"
     "      title={" (tex-safe title) "},\n"
     "      xlabel={Mean execution time ($\\mu$s)},\n"
     "      width=0.85\\textwidth, height=" chart-height-cm "cm,\n"
     "      xbar, bar width=12pt, xmode=log, xmin=1,\n"
     "      enlarge x limits={upper, value=0.15},\n"
     "      ytick={" (str/join "," (range n)) "},\n"
     "      yticklabels={"
     (str/join ","
               (map-indexed (fn [i _] (str "(\\romannumeral " (inc i) ")"))
                            scenarios))
     "},\n"
     "      xmajorgrids=true, grid style=dashed,\n"
     "      tick label style={font=\\small}, label style={font=\\small},\n"
     "      title style={font=\\small\\itshape},\n"
     "    ]\n"
     "    \\addplot[fill=blue!40!black, draw=blue!40!black,\n"
     "      error bars/.cd, x dir=both, x explicit\n"
     "    ] coordinates {\n"
     (str/join "\n"
               (map-indexed
                (fn [i s]
                  (format "      (%s,%d) +- (%s,0)"
                          (format "%.3f" (double (:mean s 0)))
                          i
                          (format "%.3f" (double (:std s 0)))))
                scenarios))
     "\n    };\n"
     "    \\end{axis}\n"
     "  \\end{tikzpicture}\n"
     "  \\vspace{0.5em}\n"
     "  \\begin{minipage}{0.85\\textwidth}\n"
     "    \\centering \\small\n"
     "    \\begin{tabular}{@{}r l@{}}\n"
     (str/join "\n"
               (map-indexed
                (fn [i s]
                  (format "      (\\romannumeral %d) & %s \\\\" (inc i) (tex-safe (:label s))))
                scenarios))
     "\n    \\end{tabular}\n"
     "  \\end{minipage}\n"
     "  \\caption{" (tex-safe title) "}\n"
     "  \\label{fig:" label "}\n"
     "\\end{figure}\n")))

(defn- build-pgf-tex [throughput history-sweep bank]
  (str
   "% Generated by opusdb.benchmark.report\n"
   "% Required packages: \\usepackage{pgfplots} \\pgfplotsset{compat=1.18}\n"
   "%                    \\usepackage{booktabs}\n\n"
   (when throughput
     (str/join "\n"
               (for [[k stem caption] throughput-charts
                     :let  [data (get throughput k)]
                     :when (seq data)]
                 (str
                  (throughput-section-table caption data)
                  "\n"
                  (pgf-throughput-chart data caption stem)
                  "\n"
                  (pgf-scalability-chart data caption stem)
                  "\n"
                  (some-> (pgf-abort-charts data caption stem) (str "\n"))))))
   (when (seq history-sweep)
     (str "\n"
          (history-sweep-table history-sweep)
          "\n"
          (history-sweep-chart history-sweep)
          "\n"))
   (when (seq (:scenarios bank))
     (pgf-bank-latency-chart
      (:scenarios bank)
      "Bank Transfer Latency"
      "bank-contention-latency"))))

(defn generate-pgf!
  ([stdout output-dir]
   (generate-pgf! stdout output-dir {}))
  ([stdout output-dir {:keys [latex-file]
                       :or   {latex-file (str output-dir "/benchmarks.tex")}}]
   (.mkdirs (io/file output-dir))
   (let [{:keys [throughput history-sweep criterium bank]}
         (parse/parse-all stdout)
         json-path (str output-dir "/bench-data.json")]
     (spit json-path
           (json/generate-string
            {:throughput    throughput
             :history-sweep history-sweep
             :criterium     criterium
             :bank          bank}
            {:pretty true}))
     (spit latex-file (build-pgf-tex throughput history-sweep bank))
     (println "  Generated JSON and LaTeX report in:" output-dir))))

(defn capture-and-generate-pgf!
  "Runs the benchmark thunk, captures stdout, and generates reports."
  ([bench-thunk output-dir]
   (capture-and-generate-pgf! bench-thunk output-dir {}))
  ([bench-thunk output-dir opts]
   (println "  (Capturing output... this will take a few minutes)")
   (let [captured-output (with-out-str (bench-thunk))]
     (generate-pgf! captured-output output-dir opts))))