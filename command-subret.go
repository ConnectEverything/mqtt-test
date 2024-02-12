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
	"os"

	"github.com/spf13/cobra"
)

type subretCommand struct {
	// message options
	messageOpts

	// test options
	repeat      int
	subscribers int
	messages      int
}

func newSubRetCommand() *cobra.Command {
	c := &subretCommand{}

	cmd := &cobra.Command{
		Use:   "subret [--flags...]",
		Short: "Publish {topics} retained messages, subscribe {repeat} times, and receive all retained messages.",
		Run:   c.run,
		Args:  cobra.NoArgs,
	}

	cmd.Flags().StringVar(&c.topic, "topic", defaultTopic(), "Base topic (prefix) for the test")
	cmd.Flags().IntVar(&c.qos, "qos", DefaultQOS, "MQTT QOS")
	cmd.Flags().IntVar(&c.size, "size", 0, "Approximate size of each message (pub adds a timestamp)")
	cmd.Flags().IntVar(&c.repeat, "repeat", 1, "Subscribe, receive retained messages, and unsubscribe N times")
	cmd.Flags().IntVar(&c.subscribers, "subscribers", 1, `Number of subscribers to run concurrently`)
	cmd.Flags().IntVar(&c.messages, "topics", 1, `Number of sub-topics to publish retained messages to`)

	return cmd
}

func (c *subretCommand) run(_ *cobra.Command, _ []string) {
	total := runSubWithPubret(c.subscribers, c.repeat, c.messages, 0, c.messageOpts, true)
	bb, _ := json.Marshal(total)
	os.Stdout.Write(bb)
}
