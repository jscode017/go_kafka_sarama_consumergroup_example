#!/bin/bash
cd ../kafka_2.13-2.4.0
kafka-topics --zookeeper 127.0.0.1:2181 --topic demo_topic --create --partitions 3 --replication-factor 1
