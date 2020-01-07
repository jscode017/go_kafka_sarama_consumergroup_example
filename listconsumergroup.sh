#!/bin/bash
cd ../kafka_2.13-2.4.0/bin
./kafka-consumer-groups.sh --describe --group ex_group --bootstrap-server 127.0.0.1:9092
./kafka-consumer-groups.sh --list --bootstrap-server 127.0.0.1:9092
