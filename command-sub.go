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

type subCommand struct {
	// message options
	messageOpts

	// test options
	repeat          int
	subscribers     int
	expectRetained  int
	expectPublished int
}

func newSubCommand() *cobra.Command {
	c := &subCommand{}

	cmd := &cobra.Command{
		Use:   "sub [--flags...]",
		Short: "Subscribe, receive all messages, unsubscribe, {repeat} times.",
		Run:   c.run,
		Args:  cobra.NoArgs,
	}

	cmd.Flags().StringVar(&c.topic, "topic", "", "Base topic for the test, will subscribe to {topic}/+")
	cmd.Flags().IntVar(&c.qos, "qos", DefaultQOS, "MQTT QOS")
	cmd.Flags().IntVar(&c.repeat, "repeat", 1, "Subscribe, receive retained messages, and unsubscribe N times")
	cmd.Flags().IntVar(&c.subscribers, "subscribers", 1, `Number of subscribers to run concurrently`)
	cmd.Flags().IntVar(&c.expectRetained, "retained", 0, `Expect to receive this many retained messages`)
	cmd.Flags().IntVar(&c.expectPublished, "messages", 0, `Expect to receive this many published messages`)

	return cmd
}

func (c *subCommand) run(_ *cobra.Command, _ []string) {
	total := runSubPrepublishRetained(c.subscribers, c.repeat, c.expectRetained, c.expectPublished, c.messageOpts, false)
	bb, _ := json.Marshal(total)
	os.Stdout.Write(bb)
}

func runSubPrepublishRetained(
	nSubscribers int,
	repeat int,
	expectRetained,
	expectPublished int,
	messageOpts messageOpts,
	prepublishRetained bool,
) *Stat {
	errCh := make(chan error)
	receiverReadyCh := make(chan struct{})
	statsCh := make(chan *Stat)

	if prepublishRetained {
		if expectPublished != 0 {
			log.Fatalf("Error: --messages is not supported with --retained")
		}

		// We need to wait for all prepublished retained messages to be processed.
		// To ensure, subscribe once before we pre-publish and receive all published
		// messages.
		r := &receiver{
			clientID:        ClientID + "-sub-init",
			filterPrefix:    messageOpts.topic,
			topic:           messageOpts.topic + "/+",
			qos:             messageOpts.qos,
			expectRetained:  0,
			expectPublished: expectRetained,
			repeat:          1,
		}
		go r.receive(receiverReadyCh, statsCh, errCh)
		<-receiverReadyCh

		// Pre-publish retained messages.
		p := &publisher{
			clientID:    ClientID + "-pub",
			messages:    expectRetained,
			topics:      expectRetained,
			messageOpts: messageOpts,
		}
		p.messageOpts.retain = true
		go p.publish(nil, errCh, true)

		// wait for the initial subscription to have received all messages
		timeout := time.NewTimer(Timeout)
		defer timeout.Stop()
		select {
		case err := <-errCh:
			log.Fatalf("Error: %v", err)
		case <-timeout.C:
			log.Fatalf("Error: timeout waiting for messages in initial subscription")
		case <-statsCh:
			// all received
		}

	}

	// Connect all subscribers (and subscribe to a wildcard topic that includes
	// all published retained messages).
	for i := 0; i < nSubscribers; i++ {
		r := &receiver{
			clientID:        ClientID + "-sub-" + strconv.Itoa(i),
			filterPrefix:    messageOpts.topic,
			topic:           messageOpts.topic + "/+",
			qos:             messageOpts.qos,
			expectRetained:  expectRetained,
			expectPublished: expectPublished,
			repeat:          repeat,
		}
		go r.receive(nil, statsCh, errCh)
	}

	// wait for the stats
	total := &Stat{
		NS: make(map[string]time.Duration),
	}
	timeout := time.NewTimer(Timeout)
	defer timeout.Stop()
	for i := 0; i < nSubscribers*repeat; i++ {
		select {
		case stat := <-statsCh:
			total.Ops += stat.Ops
			total.Bytes += stat.Bytes
			for k, v := range stat.NS {
				total.NS[k] += v
			}
		case err := <-errCh:
			log.Fatalf("Error: %v", err)
		case <-timeout.C:
			log.Fatalf("Error: timeout waiting for messages")
		}
	}
	return total
}
