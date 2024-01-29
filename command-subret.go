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
