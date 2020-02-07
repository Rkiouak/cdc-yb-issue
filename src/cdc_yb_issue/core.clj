(ns cdc-yb-issue.core
  (:gen-class)
  (:require [clojure.spec.alpha :as s]
            [clojure.java.io :as io]
            [semantic-csv.core :as sc]
            [clojure.data.csv :as cd-csv]
            [clojure.spec.gen.alpha :as g]
            [util.url-and-email :as e]
            [qbits.alia :as alia]
            [environ.core :as env]
            [taoensso.timbre :as log]
            [clojure.core.async :as async :refer [go-loop <! >! chan <!! timeout]]
            [clojure-csv.core :as csv]
            [clojure.pprint])
  (:import (org.yb.client AsyncYBClient$AsyncYBClientBuilder YBClient AsyncYBClient GetChangesResponse)
           (com.google.common.net HostAndPort)
           (com.stumbleupon.async Callback)
           (org.yb.cdc CdcService$CDCRecordPB CdcService$KeyValuePairPB)))

(def async-yb-client (atom nil))
(def yb-client (atom nil))

(def session (atom nil))

(def close-chs (atom []))

(s/def ::name #{"Alice" "Bob" "Carol" "David" "Eve" "Frank" "Garry" "Hugh" "Ian" "James" "Karen" "Louise"})
(s/def ::id uuid?)
(s/def ::website ::e/url)
(s/def ::email ::e/email)
(s/def ::user (s/keys :req [::id ::name ::email ::website]))

(def user-gen (s/gen ::user))

(defn csv-data->maps [csv-data]
  (map zipmap
       (->> (first csv-data) ;; First row is the header
            (map keyword) ;; Drop if you want string keys instead
            repeat)
       (rest csv-data)))

(defn recur-getChanges [hp table stream-id tabletId term index]
  (let [recur-chan (chan 1)
        exit-chan (chan 10)]
    (go-loop [m {:hp         hp
                 :table      table
                 :stream-id  stream-id
                 :tabletId   tabletId
                 :term       term
                 :index      index}]
      (when m
        (let [{:keys [hp table stream-id tabletId term index]} m]
          (try (.getChanges ^AsyncYBClient @async-yb-client
                            hp
                            table
                            stream-id
                            tabletId
                            term
                            index
                            (reify Callback
                              (call [this getChangesResponse]
                                (let [records (.getRecordsList (.getResp getChangesResponse))
                                      new-term (.getTerm (.getOpId (.getCheckpoint (.getResp getChangesResponse))))
                                      new-index (.getTerm (.getOpId (.getCheckpoint (.getResp getChangesResponse))))]
                                  (when (not (empty? records))
                                    (log/debug "Received " (str (count records)) " change records from YugabyteDB CDC for table: " (.getName table)))
                                  (with-open [wr (io/writer "cdc.edn" :append true)]
                                    (loop [records records
                                           c 0]
                                      (if (empty? records)
                                        (when (> c 0)
                                          (log/debug "Produced " (inc c) " records to cdc.edn from table: " (.getName table)))
                                        (let [record (first records)
                                              _ (log/debug record)
                                              changes (map (fn [^CdcService$KeyValuePairPB a]
                                                             {(keyword (String.
                                                                         (.toByteArray (.getKey a))))
                                                              (.getStringValue (.getValue a))})
                                                           (.getChangesList record))
                                              primary-keys (map (fn [^CdcService$KeyValuePairPB a]
                                                                  {(keyword (String.
                                                                              (.toByteArray (.getKey a))))
                                                                   (.getStringValue (.getValue a))}) (.getKeyList record))]
                                          (.write wr (pr-str {:table-name   (.getName table)
                                                              :primary-keys primary-keys
                                                              :new-values   changes
                                                              :time         (.toString (.getTime record))
                                                              :operation    (.toString (.getOperation record))
                                                              :term         new-term
                                                              :index        new-index
                                                              :stream-id    stream-id
                                                              :tablet-id    tabletId}))
                                          (recur (rest records) (inc c)))))))
                                (async/alt!  (timeout 100) (>! recur-chan {:hp        hp
                                                                           :table     table
                                                                           :stream-id stream-id
                                                                           :tabletId  tabletId
                                                                           :term      (.getTerm (.getOpId (.getCheckpoint (.getResp getChangesResponse))))
                                                                           :index     (.getIndex (.getOpId (.getCheckpoint (.getResp getChangesResponse))))})
                                             exit-chan nil))))
               (catch Exception e
                 (log/debug "Error in go loop"))))
        (recur (<! recur-chan))))
    exit-chan))

(defn start-yb-cdc-listeners []
  (let [table-infos (bean (.getTablesList @yb-client))
        table-ids-and-names (map #(-> % bean (select-keys [:id :name :namespace])
                                      (update :id (fn [x] (.toStringUtf8 x)))
                                      (update :namespace (fn [x] (.getName x)))) (:tableInfoList table-infos))]
    (doall (map #(assoc % :poller
                        (let [table (.openTableByUUID @yb-client (:id %))]
                          (log/debug "Polling for table: " (.getName table))
                          (doseq [tablet (.getTabletsLocations table 30000)]
                            (let [tabletId (String. (.getTabletId tablet))]
                              (swap! close-chs conj (recur-getChanges (first (:host-and-ports %)) table (:stream-id %) tabletId 0 0))))))
                (map (fn [x]
                       (assoc x
                              :stream-id
                              (.getStreamId (.createCDCStream @yb-client (first (:host-and-ports x)) (:id x)))))
                     (map
                      (fn [{:keys [id name namespace]}]
                        (.openTableByUUID @yb-client id)
                        (let [tablet-servers-list (.getTabletServersList (.listTabletServers @yb-client))]
                          {:id             id
                           :name           name
                           :keyspace       namespace
                           :host-and-ports (into []
                                                 (map (fn [x]
                                                        (let [x (bean x)]
                                                          (HostAndPort/fromParts
                                                           (:host x)
                                                           (:port x))))
                                                      tablet-servers-list))}))
                      table-ids-and-names))))))

(defn setup-yb-conn-and-session []
  (let [default-timeout 30000]
    (doseq [close-ch @close-chs]
      (async/>!! close-ch true))
    (reset! close-chs [])
    (reset! session (alia/connect (alia/cluster {:contact-points      [(or (env/env :yb-tserver) "localhost")] :metrics? false
                                                 :reconnection-policy (qbits.alia.policy.reconnection/exponential-reconnection-policy 20 1000)})))
    (reset! async-yb-client (-> (new AsyncYBClient$AsyncYBClientBuilder (or (env/env :yb-master) "localhost:7100"))
                                (.defaultAdminOperationTimeoutMs default-timeout)
                                (.defaultOperationTimeoutMs default-timeout)
                                (.defaultSocketReadTimeoutMs default-timeout)
                                (.build)))
    (reset! yb-client (new YBClient @async-yb-client))))

(defn create-tables []
  (try (alia/execute @session "CREATE KEYSPACE cdc_yb;"
                     {:result-set-fn #(into [] %)})
       (catch Exception e
         (log/debug "exception creating key space; keyspace likely already existed")
         (log/debug e)))
  (try (alia/execute @session "CREATE TABLE cdc_yb.issue_replication_users (
    id text,
    name text,
    email text,
    website text,
    PRIMARY KEY (id)) WITH transactions = { 'enabled' : true };")
       (catch Exception e
         (log/debug "exception creating table; table likely already exists")
         (log/debug e)))
  (try (alia/execute @session "CREATE TABLE cdc_yb.issue_replication (
    id text,
    value text,
    key text,
    PRIMARY KEY (id, key)) WITH transactions = { 'enabled' : true };")
       (catch Exception e
         (log/debug "exception creating table; table likely already exists")
         (log/debug e)))
  (try (alia/execute @session "CREATE TABLE cdc_yb.unmatched_changes (
    id text PRIMARY KEY,
    name text,
    website text,
    email text) WITH transactions = { 'enabled' : true };")
       (catch Exception e
         (log/debug "exception creating table; table likely already exists")
         (log/debug e)))
  (try (alia/execute @session "CREATE TABLE cdc_yb.cdc_receipts (
    time timestamp,
    table_name text,
    count text, PRIMARY KEY ((table_name), time, count));")
       (catch Exception e
         (log/debug "exception creating table; table likely already exists")
         (log/debug e))))

(defn generate-csv []
  (let [data (take 1000 (repeatedly #(g/generate user-gen)))]
    (log/debug "Start of generate-csv")
    (with-open [out-file (io/writer "data.csv")]
      (->> data
           sc/vectorize
           (cd-csv/write-csv out-file)))
    (let [batch (alia/batch
                  (reduce
                    (fn [arr {id ::id name ::name email ::email website ::website}]
                      (conj arr (alia/bind (alia/prepare @session "INSERT INTO cdc_yb.issue_replication_users (id, name, email, website) VALUES (?,?,?,?);")
                                           [(.toString id) name email website]))) [] data))]
      (alia/execute
       @session
       batch))
    (let [batch (alia/batch (reduce (fn [arr {id ::id name ::name email ::email website ::website}]
                                      (concat arr (map
                                         (fn [[k v]]
                                           (alia/bind (alia/prepare @session "INSERT INTO cdc_yb.issue_replication (id, key, value) VALUES (?,?,?);")
                                                         [(.toString id) (clojure.core/name k) v]))
                                         [[:name name] [:email email] [:website website]]))) [] data))]
      (alia/execute @session batch))))

(defn reload-csv []
  (with-open [in-file (io/reader "data.csv")]
    (let [data (->> (csv/parse-csv in-file)
                    (sc/remove-comments)
                    (sc/mappify)
                    (doall))]
      (doseq [user data]
        (let [[id name email website] (vals user)]
          (log/debug "reloading row..")
          (alia/execute @session (alia/prepare @session "INSERT INTO cdc_yb.issue_replication (id, key, value) VALUES (?,?,?);")
                        {:values        [name (.toString (java.util.UUID/randomUUID)) website email]
                         :result-set-fn #(into [] %)}))))))

(defn drop-and-recreate-tables []
  (doseq [close-ch @close-chs]
    (async/>!! close-ch true))
  (log/debug "Dropping table")
  (try (alia/execute @session "DROP TABLE cdc_yb.issue_replication;")
       (catch Exception e
         (log/debug "Exception dropping cdc_yb.issue_replication")
         (log/debug e)))
  (try (alia/execute @session "DROP TABLE cdc_yb.unmatched_changes;")
       (catch Exception e
         (log/debug "Exception dropping cdc_yb.unmatched_changes")
         (log/debug e)))
  (try (alia/execute @session "DROP TABLE cdc_yb.cdc_receipts;")
       (catch Exception e
         (log/debug "Exception dropping cdc_yb.cdc_receipts")
         (log/debug e)))
  (create-tables))

(defn -main
  [& args]
  (dotimes [_ 3]
    (setup-yb-conn-and-session)
    (drop-and-recreate-tables)
    (generate-csv)))