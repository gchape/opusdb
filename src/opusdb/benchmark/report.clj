(ns opusdb.benchmark.report
  (:require
   [clojure.java.io :as io]
   [clojure.string  :as str]
   [cheshire.core   :as json]
   [opusdb.benchmark.report.parse :as parse]))

(def ^:private throughput-charts
  [["High Contention (single ref)"
    "throughput-high-contention"
    "Throughput --- High Contention (single shared ref)"]
   ["Low Contention (isolated refs)"
    "throughput-low-contention"
    "Throughput --- Low Contention (isolated refs)"]
   ["Bank Transfer (20 accounts)"
    "throughput-bank-transfer"
    "Throughput --- Bank Transfer (20 accounts)"]
   ["Read-Heavy Mix (10% writes, 10 refs)"
    "throughput-read-heavy"
    "Throughput --- Read-Heavy Mix (10\\% writes)"]
   ["Write-Heavy Mix (90% writes, 10 refs)"
    "throughput-write-heavy"
    "Throughput --- Write-Heavy Mix (90\\% writes)"]])

(defn- pgf-scalability-chart [section-data title label]
  (str
   "\\begin{figure}[htbp]\n"
   "  \\centering\n"
   "  \\begin{tikzpicture}\n"
   "    \\begin{axis}[\n"
   "      title={" title "},\n"
   "      xlabel={Thread count},\n"
   "      ylabel={Transactions / sec},\n"
   "      width=0.75\\textwidth,\n"
   "      height=6cm,\n"
   "      xtick=data,\n"
   "      ymin=0,\n"
   "      ymajorgrids=true,\n"
   "      grid style=dashed,\n"
   "      mark=*,\n"
   "      tick label style={font=\\small},\n"
   "      label style={font=\\small},\n"
   "      title style={font=\\small\\itshape},\n"
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
   "  \\caption{" title "}\n"
   "  \\label{fig:" label "}\n"
   "\\end{figure}\n"))

(defn- pgf-latency-chart [criterium-data title label]
  (let [rows (vec criterium-data)
        n    (count rows)]
    (str
     "\\begin{figure}[htbp]\n"
     "  \\centering\n"
     "  \\begin{tikzpicture}\n"
     "    \\begin{axis}[\n"
     "      title={" title "},\n"
     "      xlabel={Mean execution time ($\\mu$s)},\n"
     "      width=0.85\\textwidth,\n"
     "      height=" (+ 3 (* n 0.7)) "cm,\n"
     "      xbar,\n"
     "      bar width=10pt,\n"
     "      xmin=0,\n"
     "      enlarge x limits={upper, value=0.15},\n"
     "      ytick={" (str/join "," (range n)) "},\n"
     "      yticklabels={"
     (str/join "," (map (fn [[lbl _]] (str "{" lbl "}")) rows))
     "},\n"
     "      xmajorgrids=true,\n"
     "      grid style=dashed,\n"
     "      tick label style={font=\\small},\n"
     "      label style={font=\\small},\n"
     "      title style={font=\\small\\itshape},\n"
     "    ]\n"
     "    \\addplot[\n"
     "      fill=blue!40!black,\n"
     "      draw=blue!40!black,\n"
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
     "    \\legend{opusdb STM}\n"
     "    \\end{axis}\n"
     "  \\end{tikzpicture}\n"
     "  \\caption{" title "}\n"
     "  \\label{fig:" label "}\n"
     "\\end{figure}\n")))

(defn- pgf-throughput-chart [section-data title label]
  (str
   "\\begin{figure}[htbp]\n"
   "  \\centering\n"
   "  \\begin{tikzpicture}\n"
   "    \\begin{axis}[\n"
   "      title={" title "},\n"
   "      xlabel={Thread count},\n"
   "      ylabel={Transactions / sec},\n"
   "      ybar,\n"
   "      bar width=18pt,\n"
   "      width=0.75\\textwidth,\n"
   "      height=7cm,\n"
   "      xtick=data,\n"
   "      ymin=0,\n"
   "      enlarge y limits={upper, value=0.15},\n"
   "      ymajorgrids=true,\n"
   "      grid style=dashed,\n"
   "      nodes near coords,\n"
   "      nodes near coords align={vertical},\n"
   "      every node near coord/.append style={font=\\tiny, anchor=south},\n"
   "      legend pos=north west,\n"
   "      tick label style={font=\\small},\n"
   "      label style={font=\\small},\n"
   "      title style={font=\\small\\itshape},\n"
   "      clip=false,\n"
   "    ]\n"
   "    \\addplot[fill=blue!40!black, draw=blue!40!black] coordinates {\n"
   (str/join "\n"
             (map (fn [{:keys [threads opusdb]}]
                    (format "      (%d,%d)" threads (or opusdb 0)))
                  section-data))
   "\n    };\n"
   "    \\legend{opusdb STM}\n"
   "    \\end{axis}\n"
   "  \\end{tikzpicture}\n"
   "  \\caption{" title "}\n"
   "  \\label{fig:" label "}\n"
   "\\end{figure}\n"))

(defn- pgf-bank-latency-chart [scenarios title label]
  (let [n (count scenarios)]
    (str
     "\\begin{figure}[htbp]\n"
     "  \\centering\n"
     "  \\begin{tikzpicture}\n"
     "    \\begin{axis}[\n"
     "      title={" title "},\n"
     "      xlabel={Mean execution time ($\\mu$s)},\n"
     "      width=0.85\\textwidth,\n"
     "      height=" (+ 3 (* n 0.8)) "cm,\n"
     "      xbar,\n"
     "      bar width=12pt,\n"
     "      xmin=0,\n"
     "      enlarge x limits={upper, value=0.15},\n"
     "      ytick={" (str/join "," (range n)) "},\n"
     "      yticklabels={" (str/join "," (map-indexed (fn [i _] (str "(\\romannumeral " (inc i) ")")) scenarios)) "},\n"
     "      xmajorgrids=true,\n"
     "      grid style=dashed,\n"
     "      tick label style={font=\\small},\n"
     "      label style={font=\\small},\n"
     "      title style={font=\\small\\itshape},\n"
     "    ]\n"
     "    \\addplot[\n"
     "      fill=blue!40!black,\n"
     "      draw=blue!40!black,\n"
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
     "    \\centering\n"
     "    \\small\n"
     "    \\begin{tabular}{@{}r l@{}}\n"
     (str/join "\n"
               (map-indexed
                (fn [i s]
                  (format "      (\\romannumeral %d) & %s \\\\" (inc i) (:label s)))
                scenarios))
     "\n    \\end{tabular}\n"
     "  \\end{minipage}\n"
     "  \\caption{" title "}\n"
     "  \\label{fig:" label "}\n"
     "\\end{figure}\n")))

(defn- build-pgf-tex [throughput criterium bank]
  (str
   "% Generated by opusdb.benchmark.report\n"
   "% Required packages in your preamble:\n"
   "%   \\usepackage{pgfplots}\n"
   "%   \\pgfplotsset{compat=1.18}\n\n"

   (when throughput
     (str/join "\n"
               (for [[k stem caption] throughput-charts
                     :let  [data (get throughput k)]
                     :when (seq data)]
                 (pgf-throughput-chart data caption stem))))

   (when throughput
     (str/join "\n"
               (for [[k stem caption] throughput-charts
                     :let  [data   (get throughput k)
                            s-stem (str stem "-scaling")
                            s-cap  (str/replace caption "Throughput" "Scalability")]
                     :when (seq data)]
                 (pgf-scalability-chart data s-cap s-stem))))

   (when (seq criterium)
     (pgf-latency-chart criterium
                        "Transaction Latency --- opusdb STM"
                        "latency-criterium"))

   (when (seq (:scenarios bank))
     (pgf-bank-latency-chart (:scenarios bank)
                             "Bank Transfer Latency by Contention Level"
                             "bank-contention-latency"))))

(defn generate-pgf!
  ([stdout output-dir]
   (generate-pgf! stdout output-dir {}))
  ([stdout output-dir {:keys [latex-file]
                       :or   {latex-file (str output-dir "/benchmarks.tex")}}]
   (.mkdirs (io/file output-dir))
   (let [{:keys [throughput criterium bank]} (parse/parse-all stdout)
         json-path (str output-dir "/bench-data.json")]
     (spit json-path (json/generate-string
                      {:throughput throughput
                       :criterium  criterium
                       :bank       bank}
                      {:pretty true}))
     (println "  wrote:" json-path)
     (spit latex-file (build-pgf-tex throughput criterium bank))
     (println "  wrote:" latex-file))))

(defn generate-pgf-from-file!
  ([input-file output-dir]
   (generate-pgf-from-file! input-file output-dir {}))
  ([input-file output-dir opts]
   (generate-pgf! (slurp input-file) output-dir opts)))

(defn capture-and-generate-pgf!
  ([bench-thunk output-dir]
   (capture-and-generate-pgf! bench-thunk output-dir {}))
  ([bench-thunk output-dir opts]
   (generate-pgf! (with-out-str (bench-thunk)) output-dir opts)))