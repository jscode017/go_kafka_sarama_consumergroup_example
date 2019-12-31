#!/bin/bash
cd ../kafka_2.13-2.4.0
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic demo_topic -- from-beginning
