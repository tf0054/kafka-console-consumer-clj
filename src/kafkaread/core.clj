(ns kafkaread.core
  (:use [clojure.pprint])
  (:require [shovel.consumer :as sh-consumer]
            [clojure.core.async :as async]))

(def kafkaserver "internal-vagrant.genn.ai:2181")

(def config {:auto.commit.interval.ms "1000",
             :zookeeper.sync.time.ms "1000",
             :zookeeper.session.timeout.ms "1000",
             :auto.offset.reset "largest",
             :zookeeper.connect kafkaserver,
             :thread.pool.size "4",
             :auto.commit.enable "true"})

(def counter (ref 0))

(defn showThreadId []
  "Getting thread-id of this processing"
  (.getId (Thread/currentThread)))

(defn countup [x]
  "increment counter"
  (dosync (alter x inc)))

(defn message-to-vec2
  "returns a hashmap of all of the message fields"
  [^kafka.message.MessageAndMetadata message]
  {:topic (.topic message),
   :offset (.offset message),
   :partition (.partition message),
   :key (.key message)
   :message (String. (.message message))})

(defn default-iterator2
  "processing all streams in a thread and printing the message field for each message"
  [^java.util.ArrayList streams x]
  ;; create a thread for each stream
  (println "main: " (showThreadId))
  (doseq
    [^kafka.consumer.KafkaStream stream streams]
    (async/thread
     (doseq
       [^kafka.message.MessageAndMetadata message stream]
       (let[cnt (countup x)]
         (println (:message (message-to-vec2 message)) cnt)
    (println "sub: " (showThreadId))
         ))))
  x
  )

(defn set-interval [callback ms]
  "common function for periodical function call"
  (future (while true (do (Thread/sleep ms) (callback)))))

;;kicked immediately..
(def pjob
  "future for resetting counter"
  (set-interval #(dosync (ref-set counter 0)) 3000))

(defn -main
  "The application's main function"
  [& args]
  (doall
   (pmap
    #((let [strTopic %
          cfg (conj config {:group.id (str "shovel-" strTopic) :topic strTopic})]
      (default-iterator2
        (sh-consumer/message-streams
         (sh-consumer/consumer-connector cfg)
         (:topic cfg)
         (int (read-string (:thread.pool.size cfg))))
        counter)))
    ["test_input_urls" "gungnir_track.544a65950cf28a00f105fb79.queryTuple"]))
  (println "main_ended?")
  ;(future-cancel pjob) ; to cancel periodical reset function call
  )
