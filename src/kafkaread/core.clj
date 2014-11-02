(ns kafkaread.core
  (:use [clojure.pprint])
  (:require
   [aleph.tcp :as tcp]
   [manifold.stream :as s]
   [clojure.tools.cli :refer [parse-opts]]
   [clojure.string :as string]
   [clojure.core.async :as async]
   [kafkaread.kafka :as kafka]))

(defn cli-parse-uiserver [x]
  (zipmap [:host :port] (string/split x #"\:")))

(defn cli-parse-topic [x]
  (string/split x #"\,"))

(def cli-options
  ;definitions of option
  ;long option should have an exampleo in it....
  [["-u" "--uiserver localhost:6666" "Graphit server address"
    :id :uiserver
    :default nil
    :parse-fn cli-parse-uiserver]
   ["-z" "--zkserver localhost:2181" "ZK server address"
    :id :zkserver
    :default "internal-vagrant.genn.ai:2181"]
   ["-t" "--topic A,B, ..." "Topics you want to count msgs"
    :id :topics
    :default nil
    :parse-fn cli-parse-topic]
   ["-r" "--reset 1000" "Reset per millsec (non-sending mode only)"
    :id :resetmil
    :default 1000]
   ["-h" "--help" "Show this help msg"]])

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn usage [options-summary]
  (->> ["This is my program. There are many like it, but this one is mine."
        ""
        "Usage: program-name [options] action"
        ""
        "Options:"
        options-summary
        ""
        "Please refer to the manual page for more information."]
       (string/join \newline)))

(defn showThreadId []
  "Getting thread-id of this processing"
  (.getId (Thread/currentThread)))

(defn set-interval [callback ms]
  "common function for periodical function call"
  (future (while true (do (Thread/sleep ms) (callback)))))

(defn resetCounters [x]
  (doall (map #(dosync (ref-set % 0)) x)))

(defn showCounters [x]
  (let [refData (ref "")]
;    (doall (map #(dosync (ref-set refData (str @refData "\n" "Graph" % "\t" % "\t" (deref %2)))) (range) x))
     (doall (map #(dosync (ref-set refData (str @refData "\n" "Graph0" "\t" % "\t" (deref %2)))) ["out" "in"] x))
    ;(println @refData)
    ;@refData))
    (let [strData @refData]
      (doall (map #(dosync (ref-set % 0)) x))
      (println strData)
      strData)))

(defn sendCounters [c x]
  (let [strData (showCounters x)]
    (s/put! c strData)
    (doall (map #(dosync (ref-set % 0)) x))))

(defn -main
  "The application's main function"
  [& args]
  (println "main: " (showThreadId))
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    ; Help
    (if (:help options)
      (exit 0 (usage summary)))
    (if (nil? (:topics options))
      (do (println "Topic(s) have to be defined.")
        (exit 0 (usage summary)))
      (println "Topics: " (string/join ", " (:topics options))))
    ; Main
    (let [objRefs (doall (pmap #(kafka/runConsumer (:zkserver options) %)
                               (:topics options)))]
      ; Timer1
      ;(set-interval #(resetCounters objRefs) (:resetmil options))
      ; Timer2
      (if (not (nil? (:uiserver options))) (do
        (println "Graphit: " (:uiserver options))
        (let [c @(tcp/client {:host "localhost", :port 6666})]
                  ;{:host (:host (:uiserver options))
                  ;            :port (:port (:uiserver options))})]
          (set-interval #(sendCounters c objRefs) 1000)))
        (set-interval #(showCounters objRefs) 1000))))
  (println "main_ended?"))
  ;(future-cancel pjob) ; to cancel periodical reset function call
