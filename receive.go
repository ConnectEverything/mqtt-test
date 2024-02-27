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
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

type receiver struct {
	dial            dial   // MQTT server to connect to.
	clientID        string // MQTT client ID.
	expectPublished int    // expect to receive this many published messages.
	expectRetained  int    // expect to receive this many retained messages.
	expectTimestamp bool   // Expect a timestamp in the payload.
	filterPrefix    string // Only count messages if their topic starts with the prefix.
	qos             int    // MQTT QOS for the subscription.
	repeat          int    // Number of times to repeat subscribe/receive/unsubscribe.
	topic           string // Subscription topic.

	// state
	cRetained     *atomic.Int32 // Count of retained messages received.
	cPublished    *atomic.Int32 // Count of published messages received.
	durPublished  *atomic.Int64 // Total duration of published messages received (measured from the sent timestamp in the message).
	bc            *atomic.Int64 // Byte count of all messages received.
	start         time.Time
	allReceivedCh chan struct{} // Signal that all expected messages have been received.
}

func (r *receiver) receive(readyCh chan struct{}, doneCh chan struct{}) {
	defer func() {
		if doneCh != nil {
			doneCh <- struct{}{}
		}
	}()

	if r.filterPrefix == "" {
		r.filterPrefix = r.topic
	}

	cl, cleanup, err := connect(r.dial, r.clientID, CleanSession)
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	for i := 0; i < r.repeat; i++ {
		// Reset the state for each iteration.
		r.cRetained = new(atomic.Int32)
		r.cPublished = new(atomic.Int32)
		r.durPublished = new(atomic.Int64)
		r.bc = new(atomic.Int64)
		r.start = time.Now()
		r.allReceivedCh = make(chan struct{})

		token := cl.Subscribe(r.topic, byte(r.qos), r.msgHandler)
		if token.Wait() && token.Error() != nil {
			log.Fatal(token.Error())
		}
		elapsed := time.Since(r.start)
		r.start = time.Now()
		recordOp(r.clientID, r.dial, "sub", 1, elapsed, 0, "Subscribed to %q", r.topic)

		// signal that the sub is ready to receive (pulished) messages.
		if readyCh != nil {
			readyCh <- struct{}{}
		}

		// wait for all messages to be received, then clean up and signal to the caller.
		<-r.allReceivedCh

		start := time.Now()
		token = cl.Unsubscribe(r.topic)
		if token.Wait() && token.Error() != nil {
			log.Fatal(token.Error())
		}
		recordOp(r.clientID, r.dial, "unsub", 1, time.Since(start), 0, "Unsubscribed from %q", r.topic)
	}
}

func (r *receiver) msgHandler(client paho.Client, msg paho.Message) {
	opts := client.OptionsReader()
	clientID := opts.ClientID()
	switch {
	case !strings.HasPrefix(msg.Topic(), r.filterPrefix):
		log.Printf("Received a QOS %d message on unexpected topic: %s\n", msg.Qos(), msg.Topic())
		return

	case msg.Duplicate():
		log.Fatalf("received unexpected duplicate message")
		return

	case msg.Retained():
		newC := r.cRetained.Add(1)
		if newC > int32(r.expectRetained) {
			log.Fatalf("received unexpected retained message")
			return
		}
		bc := r.bc.Add(int64(len(msg.Payload())))
		if newC < int32(r.expectRetained) {
			return
		}
		elapsed := time.Since(r.start)
		recordOp(r.clientID, r.dial, "rec-ret", r.expectRetained, elapsed, bc, "Received %d retained messages", r.expectRetained)
		close(r.allReceivedCh)
		return

	default:
		newC := r.cPublished.Add(1)
		if newC > int32(r.expectPublished) {
			log.Fatalf("received unexpected published message: dup:%v, topic: %s, qos:%v, retained:%v, payload: %q",
				msg.Duplicate(), msg.Topic(), msg.Qos(), msg.Retained(), msg.Payload())
			return
		}

		elapsed := time.Since(r.start)
		if r.expectTimestamp {
			v := PubValue{}
			body := msg.Payload()
			if i := bytes.IndexByte(body, '\n'); i != -1 {
				body = body[:i]
			}
			if err := json.Unmarshal(body, &v); err != nil {
				log.Fatalf("Error parsing message JSON: %v", err)
			}
			elapsed = time.Since(time.Unix(0, v.Timestamp))
		}
		recordOp(clientID, r.dial, "rec", 1, elapsed, int64(len(msg.Payload())), "Received published message on %q", msg.Topic())

		if newC < int32(r.expectPublished) {
			return
		}
		close(r.allReceivedCh)
	}
}
