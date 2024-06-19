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

type subCommand struct {
	opts        receiver
	subscribers int
}

func newSubCommand() *cobra.Command {
	c := &subCommand{}
	cmd := &cobra.Command{
		Use:   "sub [--flags...]",
		Short: "Subscribe, receive all messages, unsubscribe, {repeat} times.",
		Run:   c.run,
		Args:  cobra.NoArgs,
	}
	cmd.Flags().IntVar(&c.opts.expectPublished, "messages", 0, `Expect to receive this many published messages`)
	cmd.Flags().IntVar(&c.opts.expectRetained, "retained", 0, `Expect to receive this many retained messages`)
	cmd.Flags().BoolVar(&c.opts.expectTimestamp, "timestamp", false, "Expect a timestamp in the payload and use it to calculate receive time")
	cmd.Flags().IntVar(&c.opts.qos, "qos", DefaultQOS, "MQTT QOS")
	cmd.Flags().IntVar(&c.opts.repeat, "repeat", 1, "Subscribe, receive (retained) messages, and unsubscribe this many times")
	cmd.Flags().StringVar(&c.opts.topic, "topic", defaultTopic(), "Topic to subscribe to")
	cmd.Flags().IntVar(&c.subscribers, "subscribers", 1, `Number of subscribers to run concurrently`)

	cmd.PreRun = func(_ *cobra.Command, _ []string) {
		prefix := c.opts.topic
		i := len(prefix) - 1
		for ; i >= 0; i-- {
			if prefix[i] != '/' && prefix[i] != '#' && prefix[i] != '+' {
				break
			}
		}
		c.opts.filterPrefix = prefix[:i+1]
	}

	return cmd
}

func (c *subCommand) run(_ *cobra.Command, _ []string) {
	doneCh := make(chan struct{})

	counter := 0
	if len(Servers) > 1 || c.subscribers > 1 {
		counter = 1
	}

	for _, d := range dials(Servers) {
		for i := 0; i < c.subscribers; i++ {
			r := c.opts // copy
			r.clientID = ClientID
			if counter != 0 {
				r.clientID = r.clientID + "-" + strconv.Itoa(counter)
				counter++
			}
			r.dial = d
			go r.receive(nil, doneCh)
		}
	}

	waitN(doneCh, c.subscribers*len(Servers), "all subscribers to finish")
}
