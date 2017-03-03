# spark-streaming-kafka

### start zooker
zkServer.sh
# zkServer.cmd

### start the broker
./kafka-server-start.sh ../config/server.properties
# kafka-server-start  ../../config/server.properties

### start a producer 
./kafka-console-producer.sh --broker-list localhost:9092 --topic test

## start a consumer
./kafka-console-consumer.sh --zookeeper localhost:2181 --topic test



https://dzone.com/articles/running-apache-kafka-on-windows-os

### version
kafka_2.10-0.10.2.0
zookeeper-3.4.9