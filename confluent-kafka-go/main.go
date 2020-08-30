package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
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

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": *brokers})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
				Value:          []byte(strconv.Itoa(i)),
			}, nil)
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

	r, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": *brokers,
		"group.id":          *consumerGroup,
		"auto.offset.reset": "latest",
	})
	if err != nil {
		log.Printf("failed to create consumer err: %v", err)
		return
	}
	defer r.Close()

	r.SubscribeTopics([]string{*topic}, nil)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := r.ReadMessage(-1)
			if err != nil {
				log.Printf("read err: %v", err)
				return
			} else {
				log.Printf("msg: %s", msg.Value)
			}
		}

	}
}
