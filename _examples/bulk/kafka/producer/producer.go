// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

package producer

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/segmentio/kafka-go"
)

var (
	sides    = []string{"BUY", "SELL"}
	symbols  = []string{"ZBZX", "ZJZZT", "ZTEST", "ZVV", "ZVZZT", "ZWZZT", "ZXZZT"}
	accounts = []string{"ABC123", "LMN456", "XYZ789"}
)

func init() {
	rand.Seed(time.Now().UnixNano())
	kafka.DefaultClientID = "go-elasticsearch-kafka-demo"
}

type Producer struct {
	BrokerURL   string
	TopicName   string
	MessageRate int

	writer *kafka.Writer

	totalMessages int64
	totalErrors   int64
	totalBytes    int64
}

func (p *Producer) Run(ctx context.Context) error {
	if err := p.CreateTopic(ctx); err != nil {
		return err
	}

	p.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{p.BrokerURL},
		Topic:   p.TopicName,
	})

	var messages []kafka.Message
	for i := 1; i <= 1000; i++ {
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
			if err := p.writer.WriteMessages(ctx, messages...); err != nil {
				return err
			}
			messages = messages[:0]
		}
	}

	if len(messages) > 0 {
		if err := p.writer.WriteMessages(ctx, messages...); err != nil {
			return err
		}
	}
	p.writer.Close()

	return nil
}

func (p *Producer) CreateTopic(ctx context.Context) error {
	conn, err := net.Dial("tcp", p.BrokerURL)
	if err != nil {
		return err
	}

	return kafka.NewConn(conn, "", 0).CreateTopics(
		kafka.TopicConfig{
			Topic:             p.TopicName,
			NumPartitions:     4,
			ReplicationFactor: 1,
		})
}

func (p *Producer) Report() {
	if p.writer == nil {
		return
	}
	stats := p.writer.Stats()
	p.totalMessages += stats.Messages
	p.totalErrors += stats.Errors
	p.totalBytes += stats.Bytes
	fmt.Printf(
		"[Producer]   messages=%-*s |   bytes=%-*s    |   errors=%-*s",
		10, humanize.Comma(int64(p.totalMessages)),
		10, humanize.Bytes(uint64(p.totalBytes)),
		10, humanize.Comma(int64(p.totalErrors)))
}
