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

type pubCommand struct {
	opts       publisher
	publishers int
}

func newPubCommand() *cobra.Command {
	c := &pubCommand{}

	cmd := &cobra.Command{
		Use:   "pub [--flags...]",
		Short: "Publish MQTT messages",
		Run:   c.run,
		Args:  cobra.NoArgs,
	}

	cmd.Flags().IntVar(&c.opts.messages, "messages", 1, "Number of transactions to run, see the specific command")
	cmd.Flags().IntVar(&c.opts.mps, "mps", 1000, `Publish mps messages per second; 0 means no delay`)
	cmd.Flags().IntVar(&c.opts.qos, "qos", DefaultQOS, "MQTT QOS")
	cmd.Flags().BoolVar(&c.opts.retain, "retain", false, "Mark each published message as retained")
	cmd.Flags().IntVar(&c.opts.size, "size", 0, "Approximate size of each message (pub adds a timestamp)")
	cmd.Flags().BoolVar(&c.opts.timestamp, "timestamp", false, "Prepend a timestamp to each message")
	cmd.Flags().StringVar(&c.opts.topic, "topic", defaultTopic(), "Base topic (prefix) to publish into (/{n} will be added if --topics > 0)")
	cmd.Flags().IntVar(&c.opts.topics, "topics", 0, `Cycle through NTopics appending "/{n}"`)
	cmd.Flags().IntVar(&c.publishers, "publishers", 1, `Number of publishers to run concurrently, at --mps each`)

	return cmd
}

func (c *pubCommand) run(_ *cobra.Command, _ []string) {
	doneCh := make(chan struct{})
	for i := 0; i < c.publishers; i++ {
		p := c.opts // copy
		p.dials = dials(Servers)
		p.clientID = ClientID
		if c.publishers > 1 {
			p.clientID = p.clientID + "-" + strconv.Itoa(i)
		}
		go p.publish(doneCh)
	}

	waitN(doneCh, c.publishers, "publisher to finish")
}
