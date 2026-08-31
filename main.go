// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nuid"
	"github.com/spf13/cobra"
)

const (
	Name                     = "mqtt-test"
	Version                  = "v0.2.0"
	DefaultServer            = "tcp://localhost:1883"
	DefaultQOS               = 0
	DisconnectCleanupTimeout = 500 // milliseconds
)

var (
	ClientID string
	Password string
	Quiet    bool
	Servers  []string
	Username string
	Verbose  bool

	// RunTimeout bounds both the wait for an individual ack and the overall
	// wait for results. The old hard-coded 10s regularly aborted legitimate
	// slow runs (e.g. QoS1 against a replicated store).
	RunTimeout time.Duration
)

var disconnectedWG = sync.WaitGroup{}

func main() {
	_ = mainCmd.Execute()
	disconnectedWG.Wait()
}

var mainCmd = &cobra.Command{
	Use:     Name + " [pub|sub|subret|...] [--flags...]",
	Short:   "MQTT Test and Benchmark Utility",
	Version: Version,
}

func init() {
	mainCmd.PersistentFlags().StringVar(&ClientID, "id", Name+"-"+nuid.Next(), "MQTT client ID")
	mainCmd.PersistentFlags().StringArrayVarP(&Servers, "server", "s", []string{DefaultServer}, "MQTT endpoint as username:password@host:port")
	mainCmd.PersistentFlags().BoolVarP(&Quiet, "quiet", "q", false, "Quiet mode, only print results")
	mainCmd.PersistentFlags().BoolVarP(&Verbose, "very-verbose", "v", false, "Very verbose, print everything we can")

	mainCmd.PersistentFlags().StringArrayVar(&Servers, "servers", []string{DefaultServer}, "MQTT endpoint as username:password@host:port")
	mainCmd.PersistentFlags().MarkDeprecated("servers", "please use server instead.")
	mainCmd.PersistentFlags().DurationVar(&RunTimeout, "timeout", 60*time.Second, "Max wait for an ack and for overall results")

	mainCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		paho.CRITICAL = log.New(os.Stderr, "[MQTT CRIT] ", 0)
		if Quiet {
			Verbose = false
			log.SetOutput(io.Discard)
		}
		if !Quiet {
			paho.ERROR = log.New(os.Stderr, "[MQTT ERROR] ", 0)
		}
		if Verbose {
			paho.WARN = log.New(os.Stderr, "[MQTT WARN] ", 0)
			paho.DEBUG = log.New(os.Stderr, "[MQTT DEBUG] ", 0)
		}
	}

	mainCmd.AddCommand(newPubCommand())
	mainCmd.AddCommand(newPubSubCommand())
	mainCmd.AddCommand(newSubCommand())
	mainCmd.AddCommand(newSubRetCommand())
}

type PubValue struct {
	Seq       int   `json:"seq"`
	Timestamp int64 `json:"timestamp"`
}

type Stat struct {
	Ops   int                      `json:"ops"`
	NS    map[string]time.Duration `json:"ns"`
	Bytes int64                    `json:"bytes"`

	// Lat carries per-message ack-latency percentiles when the command
	// samples them (pub). Optional and additive: consumers that only know
	// Ops/NS/Bytes ignore it.
	Lat *LatStats `json:"lat,omitempty"`
}

// LatStats are percentiles over per-message latency samples, in nanoseconds.
type LatStats struct {
	N   int           `json:"n"`
	P50 time.Duration `json:"p50"`
	P95 time.Duration `json:"p95"`
	P99 time.Duration `json:"p99"`
	Max time.Duration `json:"max"`
}

// latencyStats computes percentiles from samples; returns nil if empty.
// Sorts in place.
func latencyStats(lats []time.Duration) *LatStats {
	if len(lats) == 0 {
		return nil
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	at := func(p float64) time.Duration {
		i := int(p * float64(len(lats)-1))
		return lats[i]
	}
	return &LatStats{
		N:   len(lats),
		P50: at(0.50),
		P95: at(0.95),
		P99: at(0.99),
		Max: lats[len(lats)-1],
	}
}

// maxLatStats returns the element-wise worse of two LatStats (nil-safe),
// used to aggregate across publishers where percentiles can not be merged.
func maxLatStats(a, b *LatStats) *LatStats {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	m := func(x, y time.Duration) time.Duration {
		if x > y {
			return x
		}
		return y
	}
	return &LatStats{
		N:   a.N + b.N,
		P50: m(a.P50, b.P50),
		P95: m(a.P95, b.P95),
		P99: m(a.P99, b.P99),
		Max: m(a.Max, b.Max),
	}
}

// logNoisy is for per-message logging on hot paths: it is a no-op unless
// --very-verbose, so the fmt work is not even performed when discarded.
func logNoisy(clientID, op string, dur time.Duration, f string, args ...interface{}) {
	if !Verbose {
		return
	}
	logOp(clientID, op, dur, f, args...)
}

func randomPayload(sz int) []byte {
	const ch = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()"
	b := make([]byte, sz)
	for i := range b {
		b[i] = ch[rand.Intn(len(ch))]
	}
	return b
}

func mqttVarIntLen(value int) int {
	c := 0
	for ; value > 0; value >>= 7 {
		c++
	}
	return c
}

func mqttPublishLen(topic string, qos byte, retained bool, msg []byte) int {
	// Compute len (will have to add packet id if message is sent as QoS>=1)
	pkLen := 2 + len(topic) + len(msg)
	if qos > 0 {
		pkLen += 2
	}
	return 1 + mqttVarIntLen(pkLen) + pkLen
}

func defaultTopic() string { return Name + "/" + nuid.Next() }

func logOp(clientID, op string, dur time.Duration, f string, args ...interface{}) {
	log.Printf("%8s %-6s %30s\t"+f, append([]any{
		fmt.Sprintf("%.3fms", float64(dur)/float64(time.Millisecond)),
		op,
		clientID + ":"},
		args...)...)
}
