package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type ConsumerGroupHandle struct {
}

func AddNewConsumerAndConsume(ctx context.Context, config *sarama.Config, wg *sync.WaitGroup, newlyAdd bool, brokerAddrs []string) {
	client, err := sarama.NewClient(brokerAddrs, config)
	if err != nil {
		panic(err)
	}
	defer client.Close() //ignore error

	consumerGroup, err := sarama.NewConsumerGroupFromClient("ex_group", client)
	if err != nil {
		log.Fatal(err)
	}
	defer consumerGroup.Close() //ignore error

	if !newlyAdd {
		wg.Done()
		wg.Wait()
	}
	//TODO: seems this is not enough, cause the rebalance only happen when you call Consume(), so still can not show that the rebalance is finish
	consumerGroupHandle := ConsumerGroupHandle{}
	log.Println("start consuming")
	go func() {
		for {
			select {
			case err, ok := <-consumerGroup.Errors():
				if !ok {
					return
				}
				log.Println(err)
				continue
			}
		}
	}()
	for {

		err := consumerGroup.Consume(ctx, []string{"demo_topic"}, &consumerGroupHandle)
		if err != nil {
			log.Fatal(err)
		}
		if ctx.Err() != nil {
			log.Fatal(err)
		}
	}
}
func main() {

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGTERM)

	brokerAddrs := []string{"localhost:9092"}
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	version, err := sarama.ParseKafkaVersion("2.4.0")
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}
	config.Version = version

	ctx, _ := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go AddNewConsumerAndConsume(ctx, config, &wg, false, brokerAddrs)
	go AddNewConsumerAndConsume(ctx, config, &wg, false, brokerAddrs)

	addOneConsumerCh := make(chan bool, 1)
	go func(addOne chan bool) {
		for {
			stdReader := bufio.NewReader(os.Stdin)
			text, _ := stdReader.ReadString('\n')
			if len(text) >= 4 && text == "add\n" {
				addOne <- true
			}
		}
	}(addOneConsumerCh)

	for {
		select {
		case <-signals:
			return
		case <-addOneConsumerCh:
			go AddNewConsumerAndConsume(ctx, config, &wg, true, brokerAddrs)
		}
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
		msg, ok := <-claim.Messages()
		if !ok {
			continue
		}
		fmt.Printf("%+v\n", session)
		log.Println("message claimed: value: ", string(msg.Value), "topic: ", msg.Topic, "Partition: ", msg.Partition, "offset: ", msg.Offset)
		session.MarkMessage(msg, "")
	}
}
