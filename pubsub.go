package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nuid"
	"github.com/spf13/cobra"
)

func init() {
	cmd := initPubSub(&cobra.Command{
		Use:   "pubsub [--flags...]",
		Short: "Subscribe and receive N published messages",
		Run:   runPubSub,
		Args:  cobra.NoArgs,
	})

	cmd.Flags().IntVar(&MPS, "mps", 1000, `Publish mps messages per second; 0 means all ASAP`)
	cmd.Flags().IntVar(&NSubscribers, "num-subscribers", 1, `Number of subscribers to run concurrently`)

	mainCmd.AddCommand(cmd)
}

func pubsubMsgHandler(topic string, errChan chan error, msgChan chan MQTTBenchmarkResult,
) func(paho.Client, paho.Message) {
	return func(client paho.Client, msg paho.Message) {
		opts := client.OptionsReader()
		clientID := opts.ClientID()
		switch {
		case msg.Topic() != topic:
			log.Printf("Received a QOS %d message on unexpected topic: %s\n", msg.Qos(), msg.Topic())
			// ignore

		case msg.Duplicate():
			errChan <- fmt.Errorf("received unexpected duplicate message")
			return

		case msg.Retained():
			errChan <- fmt.Errorf("received unexpected retained message")
			return
		}

		v := PubValue{}
		body := msg.Payload()
		if i := bytes.IndexByte(body, '\n'); i != -1 {
			body = body[:i]
		}
		if err := json.Unmarshal(body, &v); err != nil {
			log.Fatalf("Error parsing message JSON: %v", err)
		}
		elapsed := time.Since(time.Unix(0, v.Timestamp))
		msgChan <- MQTTBenchmarkResult{
			Ops:   1,
			Bytes: int64(len(msg.Payload())),
			NS:    map[string]time.Duration{"receive": elapsed},
		}
		logOp(clientID, "REC ->", elapsed, "Received %d bytes on %q, qos:%v", len(msg.Payload()), msg.Topic(), QOS)
	}
}

func runPubSub(_ *cobra.Command, _ []string) {
	clientID := ClientID
	if clientID == "" {
		clientID = Name + "-sub-" + nuid.Next()
	}
	topic := "/" + Name + "/" + nuid.Next()

	clientCleanupChan := make(chan func())
	subsChan := make(chan struct{})
	msgChan := make(chan MQTTBenchmarkResult)
	errChan := make(chan error)

	// Connect all subscribers (and subscribe)
	for i := 0; i < NSubscribers; i++ {
		id := clientID + "-" + strconv.Itoa(i)
		go func() {
			_, cleanup, err := connect(id, CleanSession, func(opts *paho.ClientOptions) {
				opts.
					SetOnConnectHandler(func(cl paho.Client) {
						start := time.Now()
						token := cl.Subscribe(topic, byte(QOS), pubsubMsgHandler(topic, errChan, msgChan))
						if token.Wait() && token.Error() != nil {
							errChan <- token.Error()
							return
						}
						logOp(id, "SUB", time.Since(start), "Subscribed to %q", topic)
						subsChan <- struct{}{}
					}).
					SetDefaultPublishHandler(func(client paho.Client, msg paho.Message) {
						errChan <- fmt.Errorf("received an unexpected message on %q", msg.Topic())
					})
			})
			if err != nil {
				errChan <- err
			} else {
				clientCleanupChan <- cleanup
			}
		}()
	}

	cConn, cSub := 0, 0
	timeout := time.NewTimer(Timeout)
	defer timeout.Stop()
	for (cConn < NSubscribers) || (cSub < NSubscribers) {
		select {
		case cleanup := <-clientCleanupChan:
			defer cleanup()
			cConn++
		case <-subsChan:
			cSub++
		case err := <-errChan:
			log.Fatal(err)
		case <-timeout.C:
			log.Fatalf("timeout waiting for connections")
		}
	}

	// ready to receive, start publishing. The publisher will exit when done, no need to wait for it.
	clientID = ClientID
	if clientID == "" {
		clientID = Name + "-pub-" + nuid.Next()
	} else {
		clientID = clientID + "-pub"
	}
	go func() {
		cl, cleanup, err := connect(clientID, CleanSession, nil)
		defer cleanup()
		if err == nil {
			_, err = publish(cl, topic)
		}
		if err != nil {
			errChan <- err
		}
	}()

	// wait for messages to arrive
	elapsed := time.Duration(0)
	bc := int64(0)
	timeout = time.NewTimer(Timeout)
	defer timeout.Stop()
	for n := 0; n < N*NSubscribers; {
		select {
		case r := <-msgChan:
			elapsed += r.NS["receive"]
			bc += r.Bytes
			n++

		case err := <-errChan:
			log.Fatalf("Error: %v", err)

		case <-timeout.C:
			log.Fatalf("Error: timeout waiting for messages")
		}
	}

	bb, _ := json.Marshal(MQTTBenchmarkResult{
		Ops:   N * NSubscribers,
		NS:    map[string]time.Duration{"receive": elapsed},
		Bytes: bc,
	})
	os.Stdout.Write(bb)
}
