// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

// +build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8/_examples/bulk/kafka/consumer"
	"github.com/elastic/go-elasticsearch/v8/_examples/bulk/kafka/producer"
)

var (
	brokerURL string

	topicName = "stocks"
	indexName = "stocks"
	numItems  = 1000
)

func init() {
	if v := os.Getenv("KAFKA_URL"); v != "" {
		brokerURL = v
	} else {
		brokerURL = "localhost:9092"
	}
}

func main() {
	log.SetFlags(0)

	var (
		wg  sync.WaitGroup
		ctx = context.Background()
	)

	done := make(chan os.Signal)
	signal.Notify(done, os.Interrupt)
	go func() { <-done; log.Println(""); os.Exit(0) }()

	producer := &producer.Producer{
		BrokerURL:   brokerURL,
		TopicName:   topicName,
		MessageRate: 1000,
	}
	_ = producer

	consumer := &consumer.Consumer{
		BrokerURL: brokerURL,
		TopicName: topicName,
		IndexName: indexName,
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		fmt.Println("Initializing...")
		for {
			select {
			case <-ticker.C:
				fmt.Print("\033[1A\r")
				producer.Report()
				fmt.Print("\033[1B\r")
				consumer.Report()
			}
		}
	}()

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := producer.Run(ctx); err != nil {
			log.Fatalf("ERROR: Producer: %s", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := consumer.Run(ctx); err != nil {
			log.Fatalf("ERROR: Consumer: %s", err)
		}
	}()
	wg.Wait()

	producer.Report()
	consumer.Report()
}
