package main

import (
	"context"
	"fmt"

	"github.com/smallnest/kafka-go"
)

func listPartition() {
	d := &kafka.Dialer{}
	ps, err := d.LookupPartitions(context.Background(), "tcp", *brokers, *topic)
	panicOnErr(err)

	for _, p := range ps {
		fmt.Printf("%d, leader: %v, isr: %v, replicas: %v\n", p.ID, p.Isr, p.Leader, p.Replicas)
	}
}
