(ns kafkaread.core
  (:use [clojure.pprint])
  (:require [shovel.consumer :as sh-consumer]
            [clojure.core.async :as async]))

(def kafkaserver "internal-vagrant.genn.ai:2181")
(def topicname "test_input_urls")

(def config {:auto.commit.interval.ms "1000",
             :zookeeper.sync.time.ms "1000",
             :zookeeper.session.timeout.ms "1000",
             :auto.offset.reset "largest",
             :topic topicname,
             :zookeeper.connect kafkaserver,
             :thread.pool.size "4",
             :group.id "shovel-test-0",
             :auto.commit.enable "true"})

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
  [^java.util.ArrayList streams]
  (let [c (async/chan)]
    ;; create a thread for each stream
    (doseq
      [^kafka.consumer.KafkaStream stream streams]
      (async/thread
       (async/>!! c
                  (doseq
                    [^kafka.message.MessageAndMetadata message stream]
                    (println (:message (message-to-vec2 message)))))))
    ;; read the channel forever
    (while true
      (async/<!! c))))

(defn -main
  "The application's main function"
  [& args]
  (default-iterator2
    (sh-consumer/message-streams
      (sh-consumer/consumer-connector config)
      (:topic config)
      (int (read-string (:thread.pool.size config))))))
