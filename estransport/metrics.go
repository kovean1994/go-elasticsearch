// Licensed to Elasticsearch B.V. under one or more agreements.
// Elasticsearch B.V. licenses this file to you under the Apache 2.0 License.
// See the LICENSE file in the project root for more information.

package estransport

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// For expvar, do something like this:
//
// expvar.Publish("go-elasticsearch", expvar.Func(func() interface{} {
// 		m, _ := es.Metrics()
// 		return m
// 	}))

// Measurable defines the interface for transports supporting metrics.
//
type Measurable interface {
	Metrics() (Metrics, error)
}

// connectionable defines the interface for transports returning a list of connections.
//
type connectionable interface {
	connections() []*Connection
}

// Metrics represents the transport metrics.
//
type Metrics struct {
	Requests  int         `json:"requests"`
	Failures  int         `json:"failures"`
	Responses map[int]int `json:"responses"`

	Connections []ConnectionMetric `json:"connections"`
}

// ConnectionMetric represents metric information for a connection.
//
type ConnectionMetric struct {
	URL       string     `json:"url"`
	Failures  int        `json:"failures,omitempty"`
	IsDead    bool       `json:"dead,omitempty"`
	DeadSince *time.Time `json:"dead_since,omitempty"`
}

// metrics represents the inner state of metrics.
//
type metrics struct {
	sync.RWMutex

	requests  int
	failures  int
	responses map[int]int

	connections []*Connection
}

// Metrics returns the transport metrics.
//
func (c *Client) Metrics() (Metrics, error) {
	if c.metrics == nil {
		return Metrics{}, errors.New("transport metrics not enabled")
	}
	c.metrics.RLock()
	defer c.metrics.RUnlock()

	if lockable, ok := c.pool.(sync.Locker); ok {
		lockable.Lock()
		defer lockable.Unlock()
	}

	m := Metrics{
		Requests:  c.metrics.requests,
		Failures:  c.metrics.failures,
		Responses: c.metrics.responses,
	}

	if pool, ok := c.pool.(connectionable); ok {
		for _, c := range pool.connections() {
			c.Lock()

			cm := ConnectionMetric{
				URL:      c.URL.String(),
				IsDead:   c.IsDead,
				Failures: c.Failures,
			}

			if !c.DeadSince.IsZero() {
				cm.DeadSince = &c.DeadSince
			}

			m.Connections = append(m.Connections, cm)
			c.Unlock()
		}
	}

	return m, nil
}

// String returns the metrics as a string.
//
func (m Metrics) String() string {
	var (
		i int
		b strings.Builder
	)
	b.WriteString("{")

	b.WriteString("Requests:")
	b.WriteString(strconv.Itoa(m.Requests))

	b.WriteString(" Failures:")
	b.WriteString(strconv.Itoa(m.Failures))

	if len(m.Responses) > 0 {
		b.WriteString(" Responses: ")
		b.WriteString("[")

		for code, num := range m.Responses {
			b.WriteString(strconv.Itoa(code))
			b.WriteString(":")
			b.WriteString(strconv.Itoa(num))
			if i+1 < len(m.Responses) {
				b.WriteString(", ")
			}
			i++
		}
		b.WriteString("]")
	}

	b.WriteString(" Connections: [")
	for i, c := range m.Connections {
		b.WriteString(c.String())
		if i+1 < len(m.Connections) {
			b.WriteString(", ")
		}
		i++
	}
	b.WriteString("]")

	b.WriteString("}")
	return b.String()
}

// String returns the connection information as a string.
//
func (cm ConnectionMetric) String() string {
	var b strings.Builder
	b.WriteString("{")
	b.WriteString(cm.URL)
	if cm.IsDead {
		fmt.Fprintf(&b, " dead=%v", cm.IsDead)
	}
	if cm.Failures > 0 {
		fmt.Fprintf(&b, " failures=%d", cm.Failures)
	}
	if cm.DeadSince != nil {
		fmt.Fprintf(&b, " dead_since=%s", cm.DeadSince.Local().Format(time.Stamp))
	}
	b.WriteString("}")
	return b.String()
}
