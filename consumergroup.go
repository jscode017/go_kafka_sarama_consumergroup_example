package main

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type ConsumerGroupHandle struct {
}

func main() {

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGTERM)

	brokersAddrs := []string{"localhost:9092"}
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	version, err := sarama.ParseKafkaVersion("2.4.0")
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config.Version = version

	client, err := sarama.NewClient(brokersAddrs, config)
	defer client.Close() //ignore error
	if err != nil {
		panic(err)
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient("ex_group", client)

	defer consumerGroup.Close() //ignore error
	if err != nil {
		panic(err)
	}

	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithCancel(context.Background())
	consumerGroupHandle := ConsumerGroupHandle{}

	go func() {
		for {

			err := consumerGroup.Consume(ctx, []string{"demo_topic"}, &consumerGroupHandle)
			if err != nil {
				log.Fatal(err)
			}
			if ctx.Err() != nil {
				log.Fatal(err)
			}
			log.Println("start consume")
		}
	}()

	select {
	case <-signals:
		return
	}
}

func (handle *ConsumerGroupHandle) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (handle *ConsumerGroupHandle) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (handle *ConsumerGroupHandle) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for {
		msg := <-claim.Messages()
		log.Println("message claimed: value: ", string(msg.Value), "topic: ", msg.Topic, "Partition: ", msg.Partition, "offset: ", msg.Offset)
		session.MarkMessage(msg, "")
	}
}
