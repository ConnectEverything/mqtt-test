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
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

type receiver struct {
	clientID        string // MQTT client ID.
	topic           string // Subscription topic.
	filterPrefix    string // Only count messages if their topic starts with the prefix.
	qos             int    // MQTT QOS for the subscription.
	expectRetained  int    // expect to receive this many retained messages.
	expectPublished int    // expect to receive this many published messages.
	repeat          int    // Number of times to repeat subscribe/receive/unsubscribe.

	cRetained    atomic.Int32 // Count of retained messages received.
	cPublished   atomic.Int32 // Count of published messages received.
	durPublished atomic.Int64 // Total duration of published messages received (measured from the sent timestamp in the message).
	bc           atomic.Int64 // Byte count of all messages received.

	start  time.Time
	errCh  chan error
	statCh chan *Stat
}

func (r *receiver) receive(readyCh chan struct{}, statCh chan *Stat, errCh chan error) {
	r.errCh = errCh
	r.statCh = make(chan *Stat)
	if r.filterPrefix == "" {
		r.filterPrefix = r.topic
	}

	cl, _, cleanup, err := connect(r.clientID, CleanSession)
	if err != nil {
		errCh <- err
		return
	}

	for i := 0; i < r.repeat; i++ {
		// Reset the stats for each iteration.
		r.start = time.Now()
		r.cRetained.Store(0)
		r.cPublished.Store(0)
		r.durPublished.Store(0)
		r.bc.Store(0)

		token := cl.Subscribe(r.topic, byte(r.qos), r.msgHandler)
		if token.Wait() && token.Error() != nil {
			errCh <- token.Error()
			return
		}
		logOp(r.clientID, "SUB", time.Since(r.start), "Subscribed to %q", r.topic)
		if readyCh != nil {
			readyCh <- struct{}{}
		}

		// wait for the stat value, then clean up and forward it to the caller. Errors are handled by the caller.
		stat := <-r.statCh
		statCh <- stat

		token = cl.Unsubscribe(r.topic)
		if token.Wait() && token.Error() != nil {
			errCh <- token.Error()
			return
		}
	}
	cleanup()
}

func (r *receiver) msgHandler(client paho.Client, msg paho.Message) {
	opts := client.OptionsReader()
	clientID := opts.ClientID()
	switch {
	case !strings.HasPrefix(msg.Topic(), r.filterPrefix):
		log.Printf("Received a QOS %d message on unexpected topic: %s\n", msg.Qos(), msg.Topic())
		return

	case msg.Duplicate():
		r.errCh <- fmt.Errorf("received unexpected duplicate message")
		return

	case msg.Retained():
		newC := r.cRetained.Add(1)
		if newC > int32(r.expectRetained) {
			r.errCh <- fmt.Errorf("received unexpected retained message")
			return
		}
		logOp(clientID, "RRET ->", time.Since(r.start), "Received %d bytes on %q, qos:%v", len(msg.Payload()), msg.Topic(), msg.Qos())
		r.bc.Add(int64(len(msg.Payload())))

		if newC < int32(r.expectRetained) {
			return
		}
		elapsed := time.Since(r.start)
		r.statCh <- &Stat{
			Ops:   1,
			NS:    map[string]time.Duration{fmt.Sprintf("rec%vret", r.expectRetained): elapsed},
			Bytes: r.bc.Load(),
		}
		return

	default:
		newC := r.cPublished.Add(1)
		if newC > int32(r.expectPublished) {
			r.errCh <- fmt.Errorf("received unexpected published message: dup:%v, topic: %s, qos:%v, retained:%v, payload: %q",
				msg.Duplicate(), msg.Topic(), msg.Qos(), msg.Retained(), msg.Payload())
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
		logOp(clientID, "RPUB ->", elapsed, "Received %d bytes on %q, qos:%v", len(msg.Payload()), msg.Topic(), msg.Qos())

		dur := r.durPublished.Add(int64(elapsed))
		bb := r.bc.Add(int64(len(msg.Payload())))
		if newC < int32(r.expectPublished) {
			return
		}
		r.statCh <- &Stat{
			Ops:   r.expectPublished,
			Bytes: bb,
			NS:    map[string]time.Duration{"receive": time.Duration(dur)},
		}
	}
}
