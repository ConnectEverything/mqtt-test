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

type pubsubCommand struct {
	pubOpts     publisher
	subOpts     receiver
	subscribers int
	pubServer   string
}

func newPubSubCommand() *cobra.Command {
	c := &pubsubCommand{}
	cmd := &cobra.Command{
		Use:   "pubsub [--flags...]",
		Short: "Subscribe and receive N published messages",
		Run:   c.run,
		Args:  cobra.NoArgs,
	}

	cmd.Flags().IntVar(&c.pubOpts.messages, "messages", 1, "Number of messages to publish and receive")
	cmd.Flags().IntVar(&c.pubOpts.mps, "mps", 1000, `Publish mps messages per second; 0 means no delay`)
	cmd.Flags().IntVar(&c.pubOpts.qos, "qos", DefaultQOS, "MQTT QOS")
	cmd.Flags().IntVar(&c.pubOpts.size, "size", 0, "Message extra payload size (in addition to the JSON timestamp)")
	cmd.Flags().StringVar(&c.pubOpts.topic, "topic", defaultTopic(), "Topic (or base topic if --topics > 1)")
	cmd.Flags().IntVar(&c.pubOpts.topics, "topics", 1, "Number of topics to use, If more than one will add /1, /2, ... to --topic when publishing, and subscribe to topic/+")
	cmd.Flags().StringVar(&c.pubServer, "pub-server", "", "Server to publish to. Defaults to the first server in --servers")
	cmd.Flags().IntVar(&c.subscribers, "subscribers", 1, `Number of subscribers to run concurrently`)

	cmd.PreRun = func(_ *cobra.Command, _ []string) {
		c.pubOpts.clientID = ClientID + "-pub"
		c.pubOpts.timestamp = true
		s := c.pubServer
		if s == "" {
			s = Servers[0]
		}
		c.pubOpts.dials = []dial{dial(s)}

		c.subOpts.clientID = ClientID + "-sub"
		c.subOpts.expectPublished = c.pubOpts.messages
		c.subOpts.expectTimestamp = true
		c.subOpts.filterPrefix = c.pubOpts.topic
		c.subOpts.qos = c.pubOpts.qos
		c.subOpts.repeat = 1
		c.subOpts.topic = c.pubOpts.topic
		if c.pubOpts.topics > 1 {
			c.subOpts.topic = c.pubOpts.topic + "/+"
		}
	}

	return cmd
}

func (c *pubsubCommand) run(_ *cobra.Command, _ []string) {
	readyCh := make(chan struct{})
	doneCh := make(chan struct{})

	counter := 0
	if len(Servers) > 1 || c.subscribers > 1 {
		counter = 1
	}
	N := c.subscribers * len(Servers)

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
			go r.receive(readyCh, doneCh)
		}
	}
	waitN(readyCh, N, "subscribers to be ready")

	// ready to receive, start publishing. Give the publisher the same done
	// channel, will wait for one more.
	go c.pubOpts.publish(doneCh)

	waitN(doneCh, N+1, "publisher and all subscribers to finish")
}
