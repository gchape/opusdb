(defproject opusdb "0.1.0-SNAPSHOT"
  :description "OpusDB: a lightweight, in-memory database written in Clojure for fast, functional data management."
  :url "https://github.com/gchape/opusdb"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [io.netty/netty-buffer "4.2.9.Final"]]
  :main opusdb.main.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
