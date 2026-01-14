(defproject opusdb "0.1.0-SNAPSHOT"
  :description "OpusDB: a lightweight, in-memory database written in Clojure for functional data."
  :url "https://github.com/gchape/opusdb"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.4"]]
  :main opusdb.main.core
  :target-path "target/%s"
  :profiles {:dev {:dependencies [[criterium "0.4.6"]]
                   :jvm-opts ["-XX:+UnlockDiagnosticVMOptions"
                              "-XX:+DebugNonSafepoints"]}
             :prod {:aot :all
                    :jvm-opts ["-Dclojure.compiler.direct-linking=true"
                               "-XX:+UseG1GC"
                               "-XX:MaxGCPauseMillis=200"
                               "-server"]}
             :uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})