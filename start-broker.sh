#!/bin/sh
#docker run -d -p 9092:9092 --name lassbroker apache/kafka:latest

docker run -d -p 9092:9092 -p 19092:19092 \
  --name lassbroker \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,PLAINTEXT_HOST://:19092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://lassbroker:9092,PLAINTEXT_HOST://localhost:19092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@lassbroker:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_NUM_PARTITIONS=1 \
  apache/kafka:latest

docker network connect fastdb_default lassbroker
sleep 2 # silence some warnings
docker exec --workdir /opt/kafka/bin/ -it lassbroker sh -c \
    "./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic lass-topic"
