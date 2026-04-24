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

(defn- pgf-latency-chart [criterium-data title label]
  (let [rows (vec criterium-data)
        n    (count rows)]
    (str
     "\\begin{figure}[htbp]\n"
     "  \\centering\n"
     "  \\begin{tikzpicture}\n"
     "    \\begin{axis}[\n"
     "      title={" (tex-safe title) "},\n"
     "      xlabel={Mean execution time ($\\mu$s)},\n"
     "      width=0.85\\textwidth, height=" (+ 3 (* n 0.7)) "cm,\n"
     "      xbar, bar width=10pt, xmin=0,\n"
     "      enlarge x limits={upper, value=0.15},\n"
     "      ytick={" (str/join "," (range n)) "},\n"
     "      yticklabels={"
     (str/join "," (map (fn [[lbl _]] (str "{" (tex-safe lbl) "}")) rows))
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
                (fn [i [_ data]]
                  (format "      (%s,%d) +- (%s,0)"
                          (format "%.3f" (double (:mean data 0)))
                          i
                          (format "%.3f" (double (:std data 0)))))
                rows))
     "\n    };\n"
     "    \\end{axis}\n"
     "  \\end{tikzpicture}\n"
     "  \\caption{" (tex-safe title) "}\n"
     "  \\label{fig:" label "}\n"
     "\\end{figure}\n")))

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

(defn- build-pgf-tex [throughput criterium bank]
  (str
   "% Generated by opusdb.benchmark.report\n"
   "% Required packages: \\usepackage{pgfplots} \\pgfplotsset{compat=1.18}\n\n"
   (when throughput
     (str/join "\n"
               (for [[k stem caption] throughput-charts
                     :let  [data (get throughput k)]
                     :when (seq data)]
                 (str (pgf-throughput-chart data caption stem)
                      "\n"
                      (pgf-scalability-chart data caption stem))))) ;; <-- Now using scalability chart!
   (when (seq criterium)
     (pgf-latency-chart criterium "Transaction Latency: opusdb STM" "latency-criterium"))
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
     (spit latex-file (build-pgf-tex throughput criterium bank))
     (println "  Generated JSON and LaTeX report in:" output-dir))))

(defn capture-and-generate-pgf!
  "Runs the benchmark thunk, captures stdout, and generates reports."
  ([bench-thunk output-dir]
   (capture-and-generate-pgf! bench-thunk output-dir {}))
  ([bench-thunk output-dir opts]
   (println "  (Capturing output... this will take a few minutes)")
   (let [captured-output (with-out-str (bench-thunk))]
     (generate-pgf! captured-output output-dir opts))))