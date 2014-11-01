(ns kafkaread.kafka
  (:use [clojure.pprint])
  (:require [shovel.consumer :as sh-consumer]
            [clojure.core.async :as async]))

;(def zkserver "internal-vagrant.genn.ai:2181")

(def config {:auto.commit.interval.ms "1000",
             :zookeeper.sync.time.ms "1000",
             :zookeeper.session.timeout.ms "1000",
             :auto.offset.reset "largest",
;             :zookeeper.connect zkserver,
             :thread.pool.size "4",
             :auto.commit.enable "true"})

(defn showThreadId []
  "Getting thread-id of this processing"
  (.getId (Thread/currentThread)))

(defn countup [x]
  "reset the counter"
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
  (doseq
    [^kafka.consumer.KafkaStream stream streams]
    (async/thread ; cannot be replaced (.start(Thread +++)) ?
     (doseq
       [^kafka.message.MessageAndMetadata message stream]
       (let[cnt (countup x)]
         (println (:message (message-to-vec2 message)) cnt "(" (showThreadId) ")")
         )))))

(defn runConsumer
  "The application's main function"
  [zkserver strTopic]
  (let [counter (ref 0)
        cfg (conj config {:zookeeper.connect zkserver
                          :group.id (str "shovel-" strTopic)
                          :topic strTopic})]
         (default-iterator2
           (sh-consumer/message-streams
            (sh-consumer/consumer-connector cfg)
            (:topic cfg)
            (int (read-string (:thread.pool.size cfg))))
           counter)
    counter))
