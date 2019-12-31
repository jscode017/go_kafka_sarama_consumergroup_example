#!/bin/bash
cd ../kafka_2.13-2.4.0
kafka-console-producer --broker-list 127.0.0.1:9092 --topic demo_topic --producer-property acks=all
