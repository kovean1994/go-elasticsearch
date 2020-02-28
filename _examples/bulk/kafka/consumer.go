// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

// +build ignore

package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

var (
	kafkaURL  string
	topicName = "stocks"

	numWorkers int
	flushBytes = 1000
	indexName  = "stocks"
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

	es, err := elasticsearch.NewClient(elasticsearch.Config{
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff:  func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
		MaxRetries:    5,
	})
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      indexName,
		Client:     es,
		NumWorkers: numWorkers,
		FlushBytes: int(flushBytes),
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	// Initialize the reader
	//
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "go-elasticsearch-demo",
		Topic:   topicName,
	})

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("ERROR: reader: %s", err)
			break
		}
		// log.Printf("%v/%v/%v:%s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))

		if err := indexer.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action: "index",
				Body:   bytes.NewReader(msg.Value),
				OnSuccess: func(item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					log.Printf("Indexed %s/%s", res.Index, res.DocumentID)
				},
				OnFailure: func(item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						log.Printf("ERROR: %s", err)
					} else {
						log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			}); err != nil {
			log.Printf("ERROR: indexer: %s", err)
		}
	}

	reader.Close()
	indexer.Close(context.Background())

	log.Printf("Reader:  %+v", reader.Stats())
	log.Printf("Indexer: %+v", indexer.Stats())
}
