package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nuid"
	"github.com/spf13/cobra"
)

func init() {
	cmd := initPubSub(&cobra.Command{
		Use:   "subret [--flags...]",
		Short: "Subscribe N times, and receive NTopics retained messages",
		Run:   runSubRet,
		Args:  cobra.NoArgs,
	})

	cmd.Flags().IntVar(&NSubscribers, "num-subscribers", 1, `Number of subscribers to run concurrently`)
	cmd.Flags().IntVar(&NTopics, "num-topics", 0, `Use this many topics with retained messages`)

	mainCmd.AddCommand(cmd)
}

func subretMsgHandler(
	topicPrefix string,
	expectNRetained int,
	start time.Time,
	doneChan chan MQTTBenchmarkResult,
) func(paho.Client, paho.Message) {
	var cRetained atomic.Int32
	var bc atomic.Int64
	return func(client paho.Client, msg paho.Message) {
		opts := client.OptionsReader()
		clientID := opts.ClientID()
		switch {
		case !strings.HasPrefix(msg.Topic(), topicPrefix):
			log.Printf("Received a QOS %d message on unexpected topic: %s\n", msg.Qos(), msg.Topic())
			// ignore

		case msg.Duplicate():
			log.Fatal("received unexpected duplicate message")
			return

		case msg.Retained():
			newC := cRetained.Add(1)
			bc.Add(int64(len(msg.Payload())))
			switch {
			case newC < int32(expectNRetained):
				logOp(clientID, "REC ->", time.Since(start), "Received %d bytes on %q, qos:%v", len(msg.Payload()), msg.Topic(), QOS)
				// skip it
				return
			case newC > int32(expectNRetained):
				log.Fatal("received unexpected retained message")
			default: // last expected retained message
				elapsed := time.Since(start)
				r := MQTTBenchmarkResult{
					Ops:   1,
					NS:    map[string]time.Duration{"receive": elapsed},
					Bytes: bc.Load(),
				}
				doneChan <- r
			}
		}
	}
}

func runSubRet(_ *cobra.Command, _ []string) {
	topic := "/" + Name + "/" + nuid.Next()

	clientID := ClientID
	if clientID == "" {
		clientID = Name + "-pub-" + nuid.Next()
	} else {
		clientID = clientID + "-pub"
	}
	nTopics := NTopics
	if nTopics == 0 {
		nTopics = 1
	}

	// Publish NTopics retained messages, 1 per topic; Use at least QoS1 to
	// ensure the retained messages are fully processed by the time the
	// publisher exits.
	cl, disconnect, err := connect(clientID, CleanSession, nil)
	if err != nil {
		log.Fatal("Error connecting: ", err)
	}
	for i := 0; i < nTopics; i++ {
		t := topic + "/" + strconv.Itoa(i)
		payload := randomPayload(Size)
		start := time.Now()
		publishQOS := 1
		if publishQOS < QOS {
			publishQOS = QOS
		}
		if token := cl.Publish(t, byte(publishQOS), true, payload); token.Wait() && token.Error() != nil {
			log.Fatal("Error publishing: ", token.Error())
		}
		logOp(clientID, "PUB <-", time.Since(start), "Published: %d bytes to %q, qos:%v, retain:%v", len(payload), t, QOS, true)
	}
	disconnect()

	// Now subscribe and verify that all subs receive all messages
	clientID = ClientID
	if clientID == "" {
		clientID = Name + "-sub-" + nuid.Next()
	}

	// Connect all subscribers (and subscribe to a wildcard topic that includes
	// all published retained messages).
	doneChan := make(chan MQTTBenchmarkResult)
	for i := 0; i < NSubscribers; i++ {
		id := clientID + "-" + strconv.Itoa(i)
		prefix := topic
		t := topic + "/+"
		go subscribeAndReceiveRetained(id, t, prefix, N, nTopics, doneChan)
	}

	var cDone int
	total := MQTTBenchmarkResult{
		NS: map[string]time.Duration{},
	}
	timeout := time.NewTimer(Timeout)
	defer timeout.Stop()

	for cDone < NSubscribers {
		select {
		case r := <-doneChan:
			total.Ops += r.Ops
			total.NS["sub"] += r.NS["sub"]
			total.NS["receive"] += r.NS["receive"]
			total.Bytes += r.Bytes
			cDone++
		case <-timeout.C:
			log.Fatalf("timeout waiting for connections")
		}
	}

	bb, _ := json.Marshal(total)
	os.Stdout.Write(bb)
}

func subscribeAndReceiveRetained(id string, subTopic string, pubTopicPrefix string, n, expected int, doneChan chan MQTTBenchmarkResult) {
	subNS := time.Duration(0)
	receiveNS := time.Duration(0)
	receiveBytes := int64(0)
	cl, cleanup, err := connect(id, CleanSession, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	for i := 0; i < n; i++ {
		start := time.Now()
		doneChan := make(chan MQTTBenchmarkResult)
		token := cl.Subscribe(subTopic, byte(QOS),
			subretMsgHandler(pubTopicPrefix, expected, start, doneChan))
		if token.Wait() && token.Error() != nil {
			log.Fatal(token.Error())
		}
		subElapsed := time.Since(start)
		subNS += subElapsed
		logOp(id, "SUB", subElapsed, "Subscribed to %q", subTopic)

		r := <-doneChan
		receiveNS += r.NS["receive"]
		receiveBytes += r.Bytes
		logOp(id, "SUBRET", r.NS["receive"], "Received %d messages (%d bytes) on %q", expected, r.Bytes, subTopic)

		// Unsubscribe
		if token = cl.Unsubscribe(subTopic); token.Wait() && token.Error() != nil {
			log.Fatal(token.Error())
		}
	}

	doneChan <- MQTTBenchmarkResult{
		Ops:   N,
		NS:    map[string]time.Duration{"sub": subNS, "receive": receiveNS},
		Bytes: receiveBytes,
	}
}
