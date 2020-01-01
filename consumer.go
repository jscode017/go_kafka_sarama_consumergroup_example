package main

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGTERM)

	brokersAddrs := []string{"localhost:9092"}
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(brokersAddrs, config)
	if err != nil {
		log.Fatal(err)
	}

	partitionConsumer, err := consumer.ConsumePartition("demo_topic", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func(context.CancelFunc, sarama.Consumer) {
		cancel()
		if err := consumer.Close(); err != nil {
			log.Fatal(err)
		}
		log.Println("successfully shut down")
	}(cancel, consumer)
	go func(ctx context.Context, partitionConsumer sarama.PartitionConsumer) {
		for {
			select {
			case <-ctx.Done():
				log.Println("context canceled")
				return
			case msg := <-partitionConsumer.Messages():
				log.Println("consume offset %d, message: %s", msg.Offset, string(msg.Value))
			case err := <-partitionConsumer.Errors():
				log.Println(err)
			}
		}
	}(ctx, partitionConsumer)
	select {
	case <-signals:
		return
	}
}
