(defproject opusdb "0.1.0-SNAPSHOT"
  :description "OpusDB: a lightweight, in-memory database written in Clojure for functional data."
  :url "https://github.com/gchape/opusdb"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :dependencies [[org.clojure/clojure "1.12.4"]]

  :main opusdb.main
  :target-path "target/%s"

  :global-vars {*warn-on-reflection* true}

  :profiles
  {:dev {:dependencies []
         :jvm-opts ["-XX:+UnlockDiagnosticVMOptions"
                    "-XX:+DebugNonSafepoints"]}

   :bench {:dependencies [[criterium "0.4.6"]
                          [cheshire "6.2.0"]]
           :jvm-opts ["-Dclojure.compiler.direct-linking=true"
                      "-Xms8g"
                      "-Xmx8g"
                      "-XX:+UseG1GC"
                      "-XX:+AlwaysPreTouch"
                      "-XX:+UnlockDiagnosticVMOptions"
                      "-XX:+DebugNonSafepoints"]}

   :jfr {:dependencies [[criterium "0.4.6"]]
         :jvm-opts ["-XX:+FlightRecorder"
                    "-XX:StartFlightRecording=name=stm,settings=profile,disk=true,filename=stm-bench.jfr"]}
   
   :prod {:aot :all
          :jvm-opts ["-Dclojure.compiler.direct-linking=true"
                     "-XX:+UseG1GC"
                     "-XX:MaxGCPauseMillis=200"]}

   :uberjar {:aot :all
             :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})