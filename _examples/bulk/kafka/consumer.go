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
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/segmentio/kafka-go"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

var (
	kafkaURL   string
	numReaders int
	topicName  = "stocks"

	numWorkers int
	flushBytes = 1000
	indexName  = "stocks"

	totalMessages int64
	totalErrors   int64
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

	// Create the Elasticsearch client
	//
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff:  func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
		MaxRetries:    5,
	})
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// Create the indexer
	//
	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      indexName,
		Client:     es,
		NumWorkers: numWorkers,
		FlushBytes: int(flushBytes),
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	// Create the reader
	//
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         []string{"localhost:9092"},
		GroupID:         "go-elasticsearch-demo",
		Topic:           topicName,
		MinBytes:        5e+5,
		MaxBytes:        5e+6,
		MaxWait:         time.Second,
		ReadLagInterval: 500 * time.Millisecond,
	})

	// Display stats
	//
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ticker.C:
				report(reader, indexer)
			}
		}
	}()

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
					// log.Printf("Indexed %s/%s", res.Index, res.DocumentID)
				},
				OnFailure: func(item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						// log.Printf("ERROR: %s", err)
					} else {
						// log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			}); err != nil {
			log.Printf("ERROR: indexer: %s", err)
		}
	}

	reader.Close()
	indexer.Close(context.Background())

	report(reader, indexer)
}

func report(reader *kafka.Reader, indexer esutil.BulkIndexer) {
	readerStats := reader.Stats()
	indexerStats := indexer.Stats()

	totalMessages += readerStats.Messages
	totalErrors += readerStats.Errors

	fmt.Printf(
		"\r[Consumer] lagging=%-*s /   received=%-*s /   errors=%-*s /   added=%-*s /   flushed=%-*s /   failed=%s",
		10, humanize.Comma(readerStats.Lag),
		10, humanize.Comma(totalMessages),
		10, humanize.Comma(totalErrors),
		10, humanize.Comma(int64(indexerStats.NumAdded)),
		10, humanize.Comma(int64(indexerStats.NumFlushed)),
		humanize.Comma(int64(indexerStats.NumFailed)))
}
