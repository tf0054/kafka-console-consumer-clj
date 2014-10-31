(ns kafkaread.core
  (:use [clojure.pprint])
  (:require
   [clojure.core.async :as async]
   [kafkaread.kafka :as kafka]))


(defn showThreadId []
  "Getting thread-id of this processing"
  (.getId (Thread/currentThread)))

(defn countup [x]
  "increment counter"
  (dosync (alter x inc)))

(defn set-interval [callback ms]
  "common function for periodical function call"
  (future (while true (do (Thread/sleep ms) (callback)))))

(defn resetCounters [x]
  (doall (map #(dosync (ref-set % 0)) x)))

(defn showCounters [x]
  (doall (map #(println "deref: " (deref %)) x)))

(defn -main
  "The application's main function"
  [& args]
  (println "main: " (showThreadId))
  (let [objRefs (doall
                 (pmap #(kafka/runConsumer %)
                       ["test_input_urls" "gungnir_track.544a65950cf28a00f105fb79.queryTuple"]))]
    ; Timers
    (set-interval #(showCounters objRefs) 1000)
    (set-interval #(resetCounters objRefs) 3000))

    ;(println "class: " (doall (class objRefs))))
  (println "main_ended?")
  ;(future-cancel pjob) ; to cancel periodical reset function call
  )
