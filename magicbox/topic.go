package main

import (
	"fmt"
	"net"
	"strconv"

	"github.com/smallnest/kafka-go"
)

// 创建topic
func createTopic() {
	//A Kafka broker allows consumers to fetch messages by topic, partition and offset.
	// Kafka brokers can create a Kafka cluster by sharing information between each other directly or indirectly
	// using Zookeeper. A Kafka cluster has exactly one broker that acts as the Controller.
	conn, err := kafka.Dial("tcp", *brokers)
	panicOnErr(err)
	defer conn.Close()

	controller, err := conn.Controller()
	panicOnErr(err)
	connOfController, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	panicOnErr(err)
	defer connOfController.Close()

	tc := kafka.TopicConfig{
		Topic:             *topic,
		NumPartitions:     64,
		ReplicationFactor: 1,
	}
	err = connOfController.CreateTopics(tc)
	fmt.Printf("create topic. err: %v\n", err)
}

func deleteTopic() {
	conn, err := kafka.Dial("tcp", *brokers)
	panicOnErr(err)
	defer conn.Close()

	controller, err := conn.Controller()
	panicOnErr(err)
	connOfController, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	panicOnErr(err)
	defer connOfController.Close()

	err = connOfController.DeleteTopics(*topic)
	fmt.Printf("delete topic. err: %v\n", err)
}

func listTopic() {
	// kafka-go 还不支持，但是pull requests已有热心网友提供了实现
}
