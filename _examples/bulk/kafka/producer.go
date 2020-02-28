// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

// +build ignore

package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/segmentio/kafka-go"
)

var (
	kafkaURL string

	totalMessages int64
	totalBytes    int64
	totalErrors   int64

	topicName = "stocks"
	numItems  = 1000

	sides    = []string{"BUY", "SELL"}
	symbols  = []string{"ZBZX", "ZJZZT", "ZTEST", "ZVV", "ZVZZT", "ZWZZT", "ZXZZT"}
	accounts = []string{"ABC123", "LMN456", "XYZ789"}
)

func init() {
	if v := os.Getenv("KAFKA_URL"); v != "" {
		kafkaURL = v
	} else {
		kafkaURL = "localhost:9092"
	}

	rand.Seed(time.Now().UnixNano())
}

func main() {
	log.SetFlags(0)

	done := make(chan os.Signal)
	signal.Notify(done, os.Interrupt)
	go func() { <-done; log.Println(""); os.Exit(0) }()

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
			NumPartitions:     4,
			ReplicationFactor: 1,
		}); err != nil {
		log.Fatalf("Error creating Kafka topic: %s", err)
	}

	// Create the writer
	//
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaURL},
		Topic:   topicName,
	})

	// Display stats
	//
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ticker.C:
				report(writer)
			}
		}
	}()

	// Build the messages
	//
	var messages []kafka.Message
	for i := 1; i <= numItems; i++ {
		var buf bytes.Buffer
		fmt.Fprintf(&buf,
			`{"symbol":"%s", "price":%d, "side":"%s", "quantity":%d, "account":"%s"}`,
			symbols[rand.Intn(len(symbols))],
			rand.Intn(1000)+5,
			sides[rand.Intn(len(sides))],
			rand.Intn(5000)+1,
			accounts[rand.Intn(len(accounts))],
		)
		messages = append(messages, kafka.Message{Value: buf.Bytes()})

		if i%100 == 0 {
			writer.WriteMessages(context.Background(), messages...)
			messages = messages[:0]
		}
	}

	// Write the messages
	//
	if len(messages) > 0 {
		writer.WriteMessages(context.Background(), messages...)
	}
	writer.Close()

	report(writer)
	log.Print("\n")
}

func report(writer *kafka.Writer) {
	stats := writer.Stats()
	totalMessages += stats.Messages
	totalBytes += stats.Bytes
	totalErrors += stats.Errors
	fmt.Printf(
		"\r[Producer] messages=%-*s /   bytes=%-*s /   errors=%s",
		len(strconv.Itoa(numItems))+4, humanize.Comma(int64(totalMessages)),
		7, humanize.Bytes(uint64(totalBytes)),
		humanize.Comma(int64(totalErrors)))
}
