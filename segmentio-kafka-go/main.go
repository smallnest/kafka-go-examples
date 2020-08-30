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

	"github.com/segmentio/kafka-go"
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

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      strings.Split(*brokers, ","),
		Topic:        *topic,
		Balancer:     &kafka.Hash{},
		Async:        false,
		BatchSize:    1,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
	})
	defer w.Close()

	for i := 0; ; i++ {
		err := w.WriteMessages(ctx,
			kafka.Message{
				Key:   nil,
				Value: []byte(strconv.Itoa(i)),
			})
		if err != nil {
			log.Printf("failed to write %d: %v", i, err)
			return
		}
		time.Sleep(time.Second)
	}
}

func read(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               strings.Split(*brokers, ","),
		GroupID:               *consumerGroup,
		Topic:                 *topic,
		MaxWait:               10 * time.Millisecond,
		MinBytes:              1,
		MaxBytes:              1024 * 1024 * 100,
		QueueCapacity:         4096,
		CommitInterval:        time.Second,
		WatchPartitionChanges: true,
		StartOffset:           kafka.LastOffset,
	})
	defer r.Close()

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			log.Printf("read err: %v", err)
			return
		} else {
			log.Printf("msg: %s", msg.Value)
		}
	}
}
