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
      (str/replace #"\$" "\\\\$")))

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
    "Throughput: Read-Heavy Mix (10% writes)"]
   ["Write-Heavy Mix (90% writes, 10 refs)"
    "throughput-write-heavy"
    "Throughput: Write-Heavy Mix (90% writes)"]])

;; ---------------------------------------------------------------------------
;; Throughput summary table
;;
;; Renders a LaTeX longtable with columns:
;;   Scenario | Threads | TPS | Aborts/commit (only when data present) | Correct
;;
;; The aborts/commit column appears only for scenarios where at least one
;; thread-count row carries the field (i.e. high-contention and write-heavy).
;; ---------------------------------------------------------------------------

(defn- has-aborts? [rows]
  (some :aborts-per-commit rows))

(defn- throughput-table-row [row show-aborts?]
  (let [tps    (or (:opusdb row) 0)
        aborts (:aborts-per-commit row)
        ok?    (:correct? row)]
    (if show-aborts?
      (format "    %d & %s & %s & %s \\\\\n"
              (:threads row)
              (tex-safe (format "%,d" (long tps)))
              (tex-safe (if aborts (format "%.2f" aborts) "---"))
              (if ok? "\\checkmark" "\\texttimes"))
      (format "    %d & %s & %s \\\\\n"
              (:threads row)
              (tex-safe (format "%,d" (long tps)))
              (if ok? "\\checkmark" "\\texttimes")))))

(defn- throughput-section-table [scenario-label rows]
  (let [show-aborts? (has-aborts? rows)
        col-spec     (if show-aborts? "rrrr" "rrr")
        header       (if show-aborts?
                       "    Threads & TPS & Aborts/commit & Correct \\\\\n"
                       "    Threads & TPS & Correct \\\\\n")]
    (str
     "\\begin{table}[htbp]\n"
     "  \\centering\n"
     "  \\caption{" (tex-safe scenario-label) "}\n"
     "  \\begin{tabular}{@{}" col-spec "@{}}\n"
     "    \\toprule\n"
     header
     "    \\midrule\n"
     (str/join (map #(throughput-table-row % show-aborts?) rows))
     "    \\bottomrule\n"
     "  \\end{tabular}\n"
     "\\end{table}\n")))

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

;; ---------------------------------------------------------------------------
;; Aborts/commit chart — rendered for high-contention and write-heavy sections
;; that carry the field.  Uses a simple line plot so it can sit beside the
;; TPS bar chart without clashing visually.
;; ---------------------------------------------------------------------------

(defn- pgf-aborts-chart [section-data title label]
  (when (has-aborts? section-data)
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
     "    ]\n"
     "    \\addplot[color=red!60!black, mark=*] coordinates {\n"
     (str/join "\n"
               (map (fn [{:keys [threads aborts-per-commit]}]
                      (format "      (%d,%.2f)" threads (or aborts-per-commit 0.0)))
                    section-data))
     "\n    };\n"
     "    \\end{axis}\n"
     "  \\end{tikzpicture}\n"
     "  \\caption{" (tex-safe (str title " — Aborts per Commit")) "}\n"
     "  \\label{fig:" label "-aborts}\n"
     "\\end{figure}\n")))

(defn- pgf-bank-latency-chart [scenarios title label]
  (let [n (count scenarios)]
    (str
     "\\begin{figure}[htbp]\n"
     "  \\centering\n"
     "  \\begin{tikzpicture}\n"
     "    \\begin{axis}[\n"
     "      title={" (tex-safe title) "},\n"
     "      xlabel={Mean execution time ($\\mu$s)},\n"
     "      width=0.85\\textwidth, height=" (+ 3 (* n 0.8)) "cm,\n"
     "      xbar, bar width=12pt, xmode=log, xmin=1,\n"
     "      enlarge x limits={upper, value=0.15},\n"
     "      ytick={" (str/join "," (range n)) "},\n"
     "      yticklabels={" (str/join "," (map-indexed (fn [i _] (str "(\\romannumeral " (inc i) ")")) scenarios)) "},\n"
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

(defn- build-pgf-tex [throughput bank]
  (str
   "% Generated by opusdb.benchmark.report\n"
   "% Required packages: \\usepackage{pgfplots} \\pgfplotsset{compat=1.18}\n"
   "%                    \\usepackage{booktabs} (for throughput tables)\n\n"
   (when throughput
     (str/join "\n"
               (for [[k stem caption] throughput-charts
                     :let  [data (get throughput k)]
                     :when (seq data)]
                 (str
                  ;; Per-scenario summary table
                  (throughput-section-table caption data)
                  "\n"
                  ;; TPS bar chart
                  (pgf-throughput-chart data caption stem)
                  "\n"
                  ;; Scalability line chart
                  (pgf-scalability-chart data caption stem)
                  "\n"
                  ;; Aborts/commit chart (only emitted when data present)
                  (some-> (pgf-aborts-chart data caption stem) (str "\n"))))))
   (when (seq (:scenarios bank))
     (pgf-bank-latency-chart (:scenarios bank) "Bank Transfer Latency" "bank-contention-latency"))))

(defn generate-pgf!
  ([stdout output-dir]
   (generate-pgf! stdout output-dir {}))
  ([stdout output-dir {:keys [latex-file]
                       :or   {latex-file (str output-dir "/benchmarks.tex")}}]
   (.mkdirs (io/file output-dir))
   (let [{:keys [throughput criterium bank]} (parse/parse-all stdout)
         json-path (str output-dir "/bench-data.json")]
     (spit json-path (json/generate-string
                      {:throughput throughput :criterium criterium :bank bank}
                      {:pretty true}))
     (spit latex-file (build-pgf-tex throughput bank))
     (println "  Generated JSON and LaTeX report in:" output-dir))))

(defn capture-and-generate-pgf!
  "Runs the benchmark thunk, captures stdout, and generates reports."
  ([bench-thunk output-dir]
   (capture-and-generate-pgf! bench-thunk output-dir {}))
  ([bench-thunk output-dir opts]
   (println "  (Capturing output... this will take a few minutes)")
   (let [captured-output (with-out-str (bench-thunk))]
     (generate-pgf! captured-output output-dir opts))))