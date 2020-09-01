package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"

	"github.com/smallnest/kafka-go"
)

func read() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   strings.Split(*brokers, ","),
		Topic:     *topic,
		Partition: rand.Intn(64),
		MinBytes:  0,    // 10KB
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(kafka.FirstOffset)
	msg, err := r.ReadMessage(context.Background())
	panicOnErr(err)
	fmt.Printf("received msg: %s, partition: %d, offset: %d", msg.Value, msg.Partition, msg.Offset)
}

func readPartition() {

}
