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
	"strconv"

	"github.com/spf13/cobra"
)

type subretCommand struct {
	pubOpts     publisher
	subOpts     receiver
	pubServers  []string
	subscribers int
}

func newSubRetCommand() *cobra.Command {
	c := &subretCommand{}
	cmd := &cobra.Command{
		Use:   "subret [--flags...]",
		Short: "Publish {topics} retained messages, subscribe {repeat} times, and receive all retained messages.",
		Run:   c.run,
		Args:  cobra.NoArgs,
	}

	cmd.Flags().IntVar(&c.pubOpts.messages, "retained", 1, "Number of retained messages to publish and receive")
	cmd.Flags().IntVar(&c.pubOpts.mps, "mps", 1000, `Publish mps messages per second; 0 means no delay`)
	cmd.Flags().IntVar(&c.pubOpts.size, "size", 0, "Message payload size")
	cmd.Flags().StringVar(&c.pubOpts.topic, "topic", defaultTopic(), "base topic (if --retained > 1 will be published to topic/1, topic/2, ...)")

	cmd.Flags().IntVar(&c.subOpts.qos, "qos", DefaultQOS, "MQTT QOS for subscriptions. Messages are published as QOS1.")
	cmd.Flags().IntVar(&c.subOpts.repeat, "repeat", 1, "Subscribe, receive retained messages, and unsubscribe N times")

	cmd.Flags().StringArrayVar(&c.pubServers, "pub-server", nil, "Server(s) to publish to. Defaults to --servers")
	cmd.Flags().IntVar(&c.subscribers, "subscribers", 1, `Number of subscribers to run concurrently`)

	cmd.PreRun = func(_ *cobra.Command, _ []string) {
		c.pubOpts.clientID = ClientID + "-pub"
		if len(c.pubServers) > 0 {
			c.pubOpts.dials = dials(c.pubServers)
		} else {
			c.pubOpts.dials = dials(Servers)
		}
		c.pubOpts.retain = true
		c.pubOpts.timestamp = false
		c.pubOpts.topics = c.pubOpts.messages
		c.pubOpts.qos = c.subOpts.qos
		// Always use at least QoS1 to ensure retained is processed.
		if c.pubOpts.qos < 1 {
			c.pubOpts.qos = 1
		}
		// Always send at least 1 byte so the messages are not treated as "retained delete".
		if c.pubOpts.size < 1 {
			c.pubOpts.size = 1
		}

		c.subOpts.clientID = ClientID + "-sub"
		c.subOpts.expectRetained = c.pubOpts.messages
		c.subOpts.expectTimestamp = false
		c.subOpts.filterPrefix = c.pubOpts.topic
		c.subOpts.topic = c.pubOpts.topic
		if c.pubOpts.topics > 1 {
			c.subOpts.topic = c.pubOpts.topic + "/+"
		}
	}

	return cmd
}

func (c *subretCommand) run(_ *cobra.Command, _ []string) {
	// Pre-publish retained messages, and wait for all to be received.
	c.pubOpts.publish(nil)

	counter := 0
	if len(Servers) > 1 || c.subscribers > 1 {
		counter = 1
	}
	N := c.subscribers * len(Servers)
	doneCh := make(chan struct{})

	// Connect all subscribers and subscribe. Wait for all subscriptions to
	// signal ready before publishing.
	for _, d := range dials(Servers) {
		for i := 0; i < c.subscribers; i++ {
			r := c.subOpts // copy
			if r.clientID == "" {
				r.clientID = ClientID
			}
			if counter != 0 {
				r.clientID = r.clientID + "-" + strconv.Itoa(counter)
				counter++
			}
			r.dial = d
			go r.receive(nil, doneCh)
		}
	}
	waitN(doneCh, N, "subscribers to finish")
}
