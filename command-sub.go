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
	total := runSubWithPubret(c.subscribers, c.repeat, c.expectRetained, c.expectPublished, c.messageOpts, false)
	bb, _ := json.Marshal(total)
	os.Stdout.Write(bb)
}
