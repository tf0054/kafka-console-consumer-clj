(ns kafkaread.core
  (:use [clojure.pprint])
  (:require
   [aleph.tcp :as tcp]
   [manifold.stream :as s]
   [clojure.tools.cli :refer [parse-opts]]
   [clojure.string :as string]
   [clojure.core.async :as async]
   [kafkaread.kafka :as kafka]))

(def cli-options
  ;; 引数が必要なオプション
   [["-s" nil "TCP sending mode"
    :id :send
    :default "localhost"]
   ;; デフォルトがnilのbooleanオプション
   ;["-h" nil "Show this help msg"]])
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
    (doall (map #(dosync (ref-set refData (str @refData "\n" "Graph" % "\t" % "\t" (deref %2)))) (range) x))
    (println @refData)
    @refData))

(defn sendCounters [c x]
  (let [strData (showCounters x)]
    (s/put! c strData)))

(defn -main
  "The application's main function"
  [& args]
  (println "main: " (showThreadId))
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    ; Help
    (if (:help options)
      (exit 0 (usage summary)))
    ; Main
    (let [objRefs (doall (pmap #(kafka/runConsumer %)
                                 ["test_input_urls" "gungnir_track.544a65950cf28a00f105fb79.queryTuple"]))]
      ; Timer1
      (set-interval #(resetCounters objRefs) 10000)
      ; Timer2
      (if (= (count arguments) 1) (do
        (let [c @(tcp/client {:host "localhost", :port 6666})]
          (set-interval #(sendCounters c objRefs) 1000)))
        (set-interval #(showCounters objRefs) 1000))))
  (println "main_ended?"))
  ;(future-cancel pjob) ; to cancel periodical reset function call
