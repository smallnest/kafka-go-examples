package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokers       = flag.String("b", "127.0.0.1:9092", "kafka brokers")
	topic         = flag.String("topic", "abc", "topic name")
	consumerGroup = flag.String("cg", "testgroup1", "consumer group")
	w             = flag.Bool("w", true, "enable write")
	r             = flag.Bool("r", true, "enable read")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	if *w {
		wg.Add(1)
		go write(ctx, &wg)
	}

	if *r {
		wg.Add(1)
		go read(ctx, &wg)
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	cancel()
	wg.Wait()

}

func write(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	config.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(strings.Split(*brokers, ","), config) // sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		panic(err)
	}

	defer p.Close()

	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			p.SendMessage(&sarama.ProducerMessage{
				Topic: *topic,
				Key:   sarama.StringEncoder(strconv.Itoa(i)),
				Value: sarama.ByteEncoder([]byte(strconv.Itoa(i))),
			})

			if err != nil {
				log.Printf("failed to write %d: %v", i, err)
				return
			}
			time.Sleep(time.Second)
		}

	}
}

func read(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	config := sarama.NewConfig()
	config.Version = sarama.MaxVersion
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	r, err := sarama.NewConsumerGroup(strings.Split(*brokers, ","), *consumerGroup, config)
	if err != nil {
		log.Printf("failed to create consumer err: %v", err)
		return
	}
	defer r.Close()

	consumer := Consumer{
		ready: make(chan bool),
		ctx:   ctx,
	}

	if err := r.Consume(ctx, strings.Split(*topic, ","), &consumer); err != nil {
		log.Printf("failed to consume: %v", err)
	}
}

type Consumer struct {
	ready chan bool
	ctx   context.Context
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-consumer.ctx.Done():
			return nil
		case msg := <-claim.Messages():
			if msg != nil {
				log.Printf("msg: %s", msg.Value)
				session.MarkMessage(msg, "")
			}

		}

	}
}
