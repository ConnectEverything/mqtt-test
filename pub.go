package main

import (
	"encoding/json"
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
		Use:   "pub [--flags...]",
		Short: "Publish N messages",
		Run:   runPub,
		Args:  cobra.NoArgs,
	})

	cmd.Flags().IntVar(&NPublishers, "num-publishers", 1, `Number of publishers to run concurrently, at --mps each`)
	cmd.Flags().IntVar(&NTopics, "num-topics", 0, `Cycle through NTopics appending "-{n}" where n starts with --num-topics-start; 0 means use --topic`)
	cmd.Flags().IntVar(&NTopicsStart, "num-topics-start", 0, `Start topic suffixes with this number (default 0)`)
	cmd.Flags().IntVar(&MPS, "mps", 1000, `Publish mps messages per second; 0 means no delay`)
	cmd.Flags().BoolVar(&Retain, "retain", false, "Mark each published message as retained")

	mainCmd.AddCommand(cmd)
}

func runPub(_ *cobra.Command, _ []string) {
	clientID := ClientID
	if clientID == "" {
		clientID = Name + "-pub-" + nuid.Next()
	}
	topic := "/" + Name + "/" + nuid.Next()
	msgChan := make(chan *MQTTBenchmarkResult)
	errChan := make(chan error)

	for i := 0; i < NPublishers; i++ {
		id := clientID + "-" + strconv.Itoa(i)
		go func() {
			cl, cleanup, err := connect(id, CleanSession, nil)
			if err != nil {
				log.Fatal(err)
			}
			defer cleanup()

			r, err := publish(cl, topic)
			if err == nil {
				msgChan <- r
			} else {
				errChan <- err
			}
		}()
	}

	// wait for messages to arrive
	pubOps := 0
	pubNS := time.Duration(0)
	pubBytes := int64(0)
	timeout := time.NewTimer(Timeout)
	defer timeout.Stop()

	// get back 1 report per publisher
	for n := 0; n < NPublishers; {
		select {
		case r := <-msgChan:
			pubOps += r.Ops
			pubNS += r.NS["pub"]
			pubBytes += r.Bytes
			n++

		case err := <-errChan:
			log.Fatalf("Error: %v", err)

		case <-timeout.C:
			log.Fatalf("Error: timeout waiting for publishers")
		}
	}

	bb, _ := json.Marshal(MQTTBenchmarkResult{
		Ops:   pubOps,
		NS:    map[string]time.Duration{"pub": pubNS},
		Bytes: pubBytes,
	})
	os.Stdout.Write(bb)
}

func publish(cl paho.Client, topic string) (*MQTTBenchmarkResult, error) {
	opts := cl.OptionsReader()
	start := time.Now()
	var elapsed time.Duration
	bc := 0
	iTopic := 0

	for n := 0; n < N; n++ {
		now := time.Now()
		if n > 0 && MPS > 0 {
			next := start.Add(time.Duration(n) * time.Second / time.Duration(MPS))
			time.Sleep(next.Sub(now))
		}

		// payload always starts with JSON containing timestamp, etc. The JSON
		// is always terminated with a '-', which can not be part of the random
		// fill. payload is then filled to the requested size with random data.
		payload := randomPayload(Size)
		structuredPayload, _ := json.Marshal(PubValue{
			Seq:       n,
			Timestamp: time.Now().UnixNano(),
		})
		structuredPayload = append(structuredPayload, '\n')
		if len(structuredPayload) > len(payload) {
			payload = structuredPayload
		} else {
			copy(payload, structuredPayload)
		}

		currTopic := topic
		if NTopics > 0 {
			currTopic = topic + "-" + strconv.Itoa(iTopic+NTopicsStart)
			iTopic++
			if iTopic >= NTopics {
				iTopic = 0
			}
		}

		startPublish := time.Now()
		if token := cl.Publish(currTopic, byte(QOS), Retain, payload); token.Wait() && token.Error() != nil {
			return nil, token.Error()
		}
		elapsedPublish := time.Since(startPublish)
		elapsed += elapsedPublish
		logOp(opts.ClientID(), "PUB <-", elapsedPublish, "Published: %d bytes to %q, qos:%v, retain:%v", len(payload), currTopic, QOS, Retain)
		bc += mqttPublishLen(currTopic, byte(QOS), Retain, payload)
	}

	return &MQTTBenchmarkResult{
		Ops:   N,
		NS:    map[string]time.Duration{"pub": elapsed},
		Bytes: int64(bc),
	}, nil
}
