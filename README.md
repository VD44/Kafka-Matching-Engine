# Kafka-Matching-Engine
Start zookeeper and kafka using Docker:
```bash
$ docker run --name zookeeper  -p 2181:2181 -d zookeeper
$ docker run --name kafka -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=$HOSTNAME:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$HOSTNAME:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -d confluentinc/cp-kafka
```      
Create Topic:
```bash
$ node topic.js
```  
Start the Test:
```bash
$ node exchange_test.js
```
In a new terminal, start a consumer to receive output:
```bash
$ node consumer.js
```
Build using maven or IDE:
```bash
$ mvn package
```
Run:
```bash
$ java -cp target/kafka-matching-engine-1.0-SNAPSHOT.jar me.App
```
