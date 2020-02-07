(defproject cdc-yb-issue "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/test.check "0.10.0"]
                 [com.cemerick/url "0.1.1"]
                 [semantic-csv "0.2.1-alpha1"]
                 [org.clojure/data.csv "0.1.4"]
                 [cc.qbits/alia-all "4.3.3"]
                 [environ "1.1.0"]
                 [org.yb.cdc "0.1.0"]
                 [com.stumbleupon/async "1.4.1"]
                 [org.jboss.netty/netty "3.2.9.Final"]
                 [com.taoensso/timbre "4.10.0"]
                 [com.fzakaria/slf4j-timbre "0.3.14"]]
  :plugins [[lein-cljfmt "0.6.6"]]
  :repositories [["local-repo" "file:lib"]]
  :main ^:skip-aot cdc-yb-issue.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
