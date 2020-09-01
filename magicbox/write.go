package main

import (
	"context"
	"math/rand"
	"strings"
	"time"

	"github.com/kr/pretty"

	"github.com/smallnest/kafka-go"
)

func write() {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  strings.Split(*brokers, ","),
		Topic:    *topic,
		Balancer: &kafka.RoundRobin{},
	})
	defer w.Close()

	for i := 0; i < 128; i++ {
		msg := time.Now().Format(time.RFC3339)
		err := w.WriteMessages(context.Background(),
			kafka.Message{
				Value: []byte(msg),
			},
		)
		panicOnErr(err)
	}

	pretty.Printf("writer stats: %v", w.Stats())
}

func writePartition() {
	partition := rand.Intn(64)
	conn, err := kafka.DialLeader(context.Background(), "tcp", *brokers, *topic, partition)
	defer conn.Close()
	conn.SetWriteDeadline(time.Now().Add(time.Second))

	msg := time.Now().Format(time.RFC3339)
	// TODO: it was blocked
	_, err = conn.WriteMessages(kafka.Message{Value: []byte(msg)})

	panicOnErr(err)
}
