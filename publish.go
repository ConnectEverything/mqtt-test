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
)

// Message options
type messageOpts struct {
	qos    int
	retain bool
	size   int
	topic  string
}

type publisher struct {
	messageOpts

	mps      int
	messages int
	topics   int
	clientID string
}

func (p *publisher) publish(msgCh chan *Stat, errorCh chan error, timestamp bool) {
	cl, _, cleanup, err := connect(p.clientID, CleanSession)
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	opts := cl.OptionsReader()
	start := time.Now()
	var elapsed time.Duration
	bc := 0
	iTopic := 0

	for n := 0; n < p.messages; n++ {
		now := time.Now()
		if n > 0 && p.mps > 0 {
			next := start.Add(time.Duration(n) * time.Second / time.Duration(p.mps))
			time.Sleep(next.Sub(now))
		}

		// payload always starts with JSON containing timestamp, etc. The JSON
		// is always terminated with a '-', which can not be part of the random
		// fill. payload is then filled to the requested size with random data.
		payload := randomPayload(p.size)
		if timestamp {
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
		if p.topics > 0 {
			currTopic = p.topic + "/" + strconv.Itoa(iTopic)
			iTopic = (iTopic + 1) % p.topics
		}

		startPublish := time.Now()
		if token := cl.Publish(currTopic, byte(p.qos), p.retain, payload); token.Wait() && token.Error() != nil {
			errorCh <- token.Error()
			return
		}
		elapsedPublish := time.Since(startPublish)
		elapsed += elapsedPublish
		logOp(opts.ClientID(), "PUB <-", elapsedPublish, "Published: %d bytes to %q, qos:%v, retain:%v", len(payload), currTopic, p.qos, p.retain)
		bc += mqttPublishLen(currTopic, byte(p.qos), p.retain, payload)
	}

	if msgCh != nil {
		msgCh <- &Stat{
			Ops:   p.messages,
			NS:    map[string]time.Duration{"pub": elapsed},
			Bytes: int64(bc),
		}
	}
}
