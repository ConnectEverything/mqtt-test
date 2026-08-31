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
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

type pubCommand struct {
	publisher
	publishers int
	timestamp  bool
}

func newPubCommand() *cobra.Command {
	c := &pubCommand{}

	cmd := &cobra.Command{
		Use:   "pub [--flags...]",
		Short: "Publish MQTT messages",
		Run:   c.run,
		Args:  cobra.NoArgs,
	}

	// Message options
	cmd.Flags().StringVar(&c.topic, "topic", defaultTopic(), "Base topic (prefix) to publish into (/{n} will be added if --topics > 0)")
	cmd.Flags().IntVar(&c.qos, "qos", DefaultQOS, "MQTT QOS")
	cmd.Flags().IntVar(&c.size, "size", 0, "Approximate size of each message (pub adds a timestamp)")
	cmd.Flags().BoolVar(&c.retain, "retain", false, "Mark each published message as retained")
	cmd.Flags().BoolVar(&c.timestamp, "timestamp", false, "Prepend a timestamp to each message")

	// Test options
	cmd.Flags().IntVar(&c.mps, "mps", 1000, `Publish mps messages per second; 0 means no delay`)
	cmd.Flags().IntVar(&c.messages, "messages", 1, "Number of transactions to run, see the specific command")
	cmd.Flags().IntVar(&c.pipeline, "pipeline", 1, `Max QoS1/2 publishes in flight per publisher; 1 means wait for each ack (measures per-publish latency), >1 measures wall-clock throughput and requires --mps 0`)
	cmd.Flags().IntVar(&c.publishers, "publishers", 1, `Number of publishers to run concurrently, at --mps each`)
	cmd.Flags().IntVar(&c.topics, "topics", 0, `Cycle through NTopics appending "/{n}"`)

	return cmd
}

func (c *pubCommand) run(_ *cobra.Command, _ []string) {
	// In pipelined mode the reported "pub" duration is total wall time, so
	// an --mps throttle would be folded into the result and make it neither
	// a latency nor a throughput measurement.
	if c.pipeline > 1 && c.mps != 0 {
		log.Fatal("--pipeline > 1 measures wall-clock throughput and requires --mps 0")
	}

	msgChan := make(chan *Stat)
	errChan := make(chan error)

	// runStart bounds the whole concurrent fan-out. With one publisher this
	// equals its own "pub" duration; with --publishers N the per-publisher
	// durations overlap, so their sum is not wall time. Aggregate throughput
	// (ops over real elapsed) must divide by this, not by the summed "pub".
	runStart := time.Now()
	for i := 0; i < c.publishers; i++ {
		p := c.publisher // copy
		p.clientID = ClientID + "-" + strconv.Itoa(i)
		go p.publish(msgChan, errChan, c.timestamp)
	}

	pubOps := 0
	pubNS := time.Duration(0)
	pubBytes := int64(0)
	var lat *LatStats
	timeout := time.NewTimer(RunTimeout)
	defer timeout.Stop()

	// get back 1 report per publisher
	for n := 0; n < c.publishers; {
		select {
		case stat := <-msgChan:
			pubOps += stat.Ops
			pubNS += stat.NS["pub"]
			pubBytes += stat.Bytes
			// Percentiles can not be merged exactly; report the worst
			// publisher's, which is conservative. With one publisher (the
			// common benchmarking case) this is simply its stats.
			lat = maxLatStats(lat, stat.Lat)
			n++

		case err := <-errChan:
			log.Fatalf("Error: %v", err)

		case <-timeout.C:
			log.Fatalf("Error: timeout waiting for publishers")
		}
	}

	bb, _ := json.Marshal(Stat{
		Ops:   pubOps,
		NS:    map[string]time.Duration{"pub": pubNS, "wall": time.Since(runStart)},
		Bytes: pubBytes,
		Lat:   lat,
	})
	os.Stdout.Write(bb)
}
