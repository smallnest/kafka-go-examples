# kafka-go-examples

examples for kafka go clients.

There are three examples which use the below kafka libs which can access kafka:

- [segmentio/kafka-go](https://github.com/segmentio/kafka-go): It provides both low and high level APIs for interacting with Kafka, easy to use and integrate with existing software.
- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go): a cgo based wrapper around librdkafka, which means it introduces a dependency to a C library on all Go code that uses the package. It has much better documentation than sarama but still lacks support for Go contexts.
- [Shopify/sarama](https://github.com/Shopify/sarama): the most popular but is quite difficult to work with. It is poorly documented, the API exposes low level concepts of the Kafka protocol, and it doesn't support recent Go features like contexts. 
