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
	"log"
	"strconv"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

type publisher struct {
	clientID  string
	dials     []dial
	messages  int    // send this many messages
	mps       int    // send at this rate (messages per second)
	qos       int    // MQTT QOS
	retain    bool   // Mark each published message as retained
	size      int    // Approximate size of each message (may add a timestamp)
	timestamp bool   // Prepend a timestamp to each message
	topic     string // Base topic (prefix) to publish into (/{n} will be added if --topics > 0)
	topics    int    // Cycle through this many topics appending "/{n}"
}

func (p *publisher) publish(doneCh chan struct{}) {
	defer func() {
		if doneCh != nil {
			doneCh <- struct{}{}
		}
	}()

	// Connect to all servers.
	clients := make([]paho.Client, len(p.dials))
	for i, d := range p.dials {
		id := p.clientID
		if len(p.dials) > 1 {
			id = p.clientID + "-" + strconv.Itoa(i)
		}

		cl, cleanup, err := connect(dial(d), id, CleanSession)
		if err != nil {
			log.Fatal(err)
		}
		defer cleanup()
		clients[i] = cl
	}

	start := time.Now()
	iTopic := 0
	iServer := 0
	for n := 0; n < p.messages; n++ {
		// Get a round-robin client.
		i := iServer % len(p.dials)
		iServer++
		d := dial(p.dials[i])
		cl := clients[i]
		now := time.Now()
		if n > 0 && p.mps > 0 {
			next := start.Add(time.Duration(n) * time.Second / time.Duration(p.mps))
			time.Sleep(next.Sub(now))
		}

		// payload always starts with JSON containing timestamp, etc. The JSON
		// is always terminated with a '-', which can not be part of the random
		// fill. payload is then filled to the requested size with random data.
		payload := randomPayload(p.size)
		if p.timestamp {
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
		}

		currTopic := p.topic
		if p.topics > 1 {
			currTopic = p.topic + "/" + strconv.Itoa(iTopic)
			iTopic = (iTopic + 1) % p.topics
		}

		startPublish := time.Now()
		if token := cl.Publish(currTopic, byte(p.qos), p.retain, payload); token.Wait() && token.Error() != nil {
			log.Fatal(token.Error())
			return
		}

		elapsedPublish := time.Since(startPublish)
		pubBytes := mqttPublishLen(currTopic, byte(p.qos), p.retain, payload)
		opts := cl.OptionsReader()

		recordOp(opts.ClientID(), d, "pub", 1, elapsedPublish, pubBytes, "<- Published: %d bytes to %q, qos:%v, retain:%v", pubBytes, currTopic, p.qos, p.retain)
	}

}
