// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

package consumer

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/segmentio/kafka-go"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type Consumer struct {
	BrokerURL  string
	TopicName  string
	IndexName  string
	NumWorkers int
	FlushBytes int

	reader  *kafka.Reader
	indexer esutil.BulkIndexer

	totalMessages int64
	totalErrors   int64
	totalBytes    int64
}

func (c *Consumer) Run(ctx context.Context) (err error) {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff:  func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
		MaxRetries:    5,
	})
	if err != nil {
		return fmt.Errorf("elasticsearch: %s", err)
	}

	c.indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      c.IndexName,
		Client:     es,
		NumWorkers: c.NumWorkers,
		FlushBytes: int(c.FlushBytes),
	})
	if err != nil {
		return fmt.Errorf("indexer: %s", err)
	}

	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:         []string{c.BrokerURL},
		GroupID:         "go-elasticsearch-demo",
		Topic:           c.TopicName,
		MinBytes:        5e+5,
		MaxBytes:        5e+6,
		MaxWait:         time.Second,
		ReadLagInterval: 500 * time.Millisecond,
	})

	for {
		msg, err := c.reader.ReadMessage(context.Background())
		if err != nil {
			return fmt.Errorf("reader: %s", err)
		}
		// log.Printf("%v/%v/%v:%s\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))

		if err := c.indexer.Add(
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
			return fmt.Errorf("indexer: %s", err)
		}
	}

	c.reader.Close()
	c.indexer.Close(context.Background())

	return nil
}

func (c *Consumer) Report() {
	if c.reader == nil || c.indexer == nil {
		return
	}
	readerStats := c.reader.Stats()
	indexerStats := c.indexer.Stats()

	c.totalMessages += readerStats.Messages
	c.totalErrors += readerStats.Errors

	fmt.Printf(
		"[Consumer]   lagging=%-*s  |   received=%-*s |   errors=%-*s |   added=%-*s |   flushed=%-*s |   failed=%s",
		10, humanize.Comma(readerStats.Lag),
		10, humanize.Comma(c.totalMessages),
		10, humanize.Comma(c.totalErrors),
		10, humanize.Comma(int64(indexerStats.NumAdded)),
		10, humanize.Comma(int64(indexerStats.NumFlushed)),
		humanize.Comma(int64(indexerStats.NumFailed)))
}
