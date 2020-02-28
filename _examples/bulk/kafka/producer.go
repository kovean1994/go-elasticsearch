// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

// +build ignore

package main

import (
	"bytes"
	"context"
	"log"
	"net"
	"os"
	"sync"

	"github.com/segmentio/kafka-go"
)

var (
	kafkaURL string

	topicName = "stocks"
	numItems  = 10000

	bufPool = sync.Pool{New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 100)) }}
)

func init() {
	if v := os.Getenv("KAFKA_URL"); v != "" {
		kafkaURL = v
	} else {
		kafkaURL = "localhost:9092"
	}
}

func main() {
	log.SetFlags(0)

	conn, err := net.Dial("tcp", kafkaURL)
	if err != nil {
		log.Fatalf("Error connecting to Kafka: %s", err)
	}

	kafka.DefaultClientID = "go-elasticsearch-kafka-demo"

	// Create the "stocks" topic
	//
	if err := kafka.NewConn(conn, "", 0).CreateTopics(
		kafka.TopicConfig{
			Topic:             topicName,
			NumPartitions:     10,
			ReplicationFactor: 1,
		}); err != nil {
		log.Fatalf("Error creating Kafka topic: %s", err)
	}

	// Initialize the writer
	//
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaURL},
		Topic:   topicName,
	})

	// Build the messages
	//
	var messages []kafka.Message
	for i := 1; i <= numItems; i++ {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.WriteString(`{"symbol":"ZBZX", "price":100, "side":"SELL", "quantity":500, "account":"ABC123"}`)
		messages = append(messages, kafka.Message{Value: buf.Bytes()})
		buf.Reset()
		bufPool.Put(buf)
	}

	// Write the messages
	//
	writer.WriteMessages(context.Background(), messages...)
	writer.Close()

	log.Printf("%+v", writer.Stats())
}
