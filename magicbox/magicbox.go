package main

import (
	"flag"
	"fmt"

	"github.com/segmentio/kafka-go"
)

var (
	brokers       = flag.String("b", "127.0.0.1:9092", "kafka brokers addresses")
	topic         = flag.String("topic", "test1", "topic name")
	consumerGroup = flag.String("cg", "testgroup1", "consumer group name")
)

func main() {
	flag.Parse()

	read()
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func info() {
	conn, err := kafka.Dial("tcp", *brokers)
	panicOnErr(err)
	defer conn.Close()

	bs, err := conn.Brokers()
	panicOnErr(err)
	var brs []string
	for _, b := range bs {
		brs = append(brs, fmt.Sprintf("%s:%d", b.Host, b.Port))
	}

	// The controller is the brain of Apache Kafka. A big part of what the controller does is to maintain the
	// consistency of the replicas and determine which replica can be used to serve the clients,
	// especially during individual broker failure.
	controller, err := conn.Controller()
	panicOnErr(err)
	controllerName := fmt.Sprintf("%s:%d", controller.Host, controller.Port)

	// apiVersions, err := conn.ApiVersions()
	// panicOnErr(err)
	fmt.Printf("brokers: %s\ncontroller: %s",
		brs, controllerName)
}
