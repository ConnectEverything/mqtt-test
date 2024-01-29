package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

type pubsubCommand struct {
	messageOpts

	messages    int
	subscribers int
}

func newPubSubCommand() *cobra.Command {
	c := &pubsubCommand{}

	cmd := &cobra.Command{
		Use:   "pubsub [--flags...]",
		Short: "Subscribe and receive N published messages",
		Run:   c.run,
		Args:  cobra.NoArgs,
	}

	// Message options
	cmd.Flags().IntVar(&c.messages, "messages", 1, "Number of messages to publish and receive")
	cmd.Flags().StringVar(&c.topic, "topic", defaultTopic(), "Topic to publish and subscribe to")
	cmd.Flags().IntVar(&c.qos, "qos", DefaultQOS, "MQTT QOS")
	cmd.Flags().IntVar(&c.size, "size", 0, "Approximate size of each message (pub adds a timestamp)")

	// Test options
	cmd.Flags().IntVar(&c.subscribers, "subscribers", 1, `Number of subscribers to run concurrently`)

	return cmd
}

func (c *pubsubCommand) run(_ *cobra.Command, _ []string) {
	clientID := ClientID + "-sub"
	readyCh := make(chan struct{})
	errCh := make(chan error)
	statsCh := make(chan *Stat)

	// Connect all subscribers (and subscribe)
	for i := 0; i < c.subscribers; i++ {
		r := &receiver{
			clientID:        clientID + "-" + strconv.Itoa(i),
			topic:           c.topic,
			qos:             c.qos,
			expectPublished: c.messages,
			repeat:          1,
		}
		go r.receive(readyCh, statsCh, errCh)
	}

	// Wait for all subscriptions to signal ready
	cSub := 0
	timeout := time.NewTimer(Timeout)
	defer timeout.Stop()
	for cSub < c.subscribers {
		select {
		case <-readyCh:
			cSub++
		case err := <-errCh:
			log.Fatal(err)
		case <-timeout.C:
			log.Fatalf("timeout waiting for subscribers to be ready")
		}
	}

	// ready to receive, start publishing. The publisher will exit when done, no need to wait for it.
	p := &publisher{
		clientID:    ClientID + "-pub",
		messageOpts: c.messageOpts,
		messages:    c.messages,
		mps:         1000,
	}
	go p.publish(nil, errCh, true)

	// wait for the stats
	total := Stat{
		NS: make(map[string]time.Duration),
	}
	timeout = time.NewTimer(Timeout)
	defer timeout.Stop()
	for i := 0; i < c.subscribers; i++ {
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

	bb, _ := json.Marshal(total)
	os.Stdout.Write(bb)
}
