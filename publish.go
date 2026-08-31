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
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
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
	pipeline int
	clientID string
}

// inflight is one pipelined publish awaiting its ack (PUBACK for QoS1,
// PUBCOMP for QoS2 — paho's token completes at the end of either flow, so
// the same pipeline measures both).
type inflight struct {
	token  mqtt.Token
	sentAt time.Time
	plen   int
}

func (p *publisher) publish(msgCh chan *Stat, errorCh chan error, timestamp bool) {
	cl, _, cleanup, err := connect(p.clientID, CleanSession)
	if err != nil {
		errorCh <- err
		return
	}
	defer cleanup()

	opts := cl.OptionsReader()

	// Payload generation is hoisted out of the measured loop: one random
	// template, generated up front. Without a timestamp the same buffer is
	// shared by every publish (it is never mutated after handoff); with a
	// timestamp each message gets a copy with the JSON header stamped in,
	// which costs a copy but no per-byte rand inside the timed window.
	template := randomPayload(p.size)
	makePayload := func(n int) []byte {
		if !timestamp {
			return template
		}
		structuredPayload, _ := json.Marshal(PubValue{
			Seq:       n,
			Timestamp: time.Now().UnixNano(),
		})
		structuredPayload = append(structuredPayload, '\n')
		if len(structuredPayload) >= len(template) {
			return structuredPayload
		}
		payload := make([]byte, len(template))
		copy(payload, template)
		copy(payload, structuredPayload)
		return payload
	}

	var elapsed time.Duration
	var lats []time.Duration
	if p.messages > 0 {
		lats = make([]time.Duration, 0, p.messages)
	}
	bc := 0
	iTopic := 0

	// With --pipeline > 1, keep up to that many QoS1/2 publishes in flight
	// instead of waiting for each ack. The buffered channel is the window:
	// the send blocks when it is full, and a single collector goroutine
	// waits the tokens in FIFO order, releasing the oldest slot first
	// (TCP-like window semantics). The reported "pub" duration is total
	// wall time, so ms/op reflects throughput; per-message ack latency is
	// sampled as publish->token-completion, which is exact when the broker
	// acks in order (NATS does; the spec does not require it). Only
	// acknowledged publishes count toward Ops/Bytes; the first error stops
	// the run and is reported after the in-flight tokens are drained.
	var okOps, okBytes int64
	var pubErr atomic.Pointer[error]
	setErr := func(err error) {
		pubErr.CompareAndSwap(nil, &err)
	}
	pipelined := p.pipeline > 1
	var window chan inflight
	collectorDone := make(chan struct{})
	if pipelined {
		window = make(chan inflight, p.pipeline)
		go func() {
			defer close(collectorDone)
			for f := range window {
				if pubErr.Load() != nil {
					continue // draining after an error; don't wait tokens
				}
				if !f.token.WaitTimeout(RunTimeout) {
					setErr(fmt.Errorf("timeout (%v) waiting for the ack", RunTimeout))
					continue
				}
				if err := f.token.Error(); err != nil {
					setErr(err)
					continue
				}
				okOps++
				okBytes += int64(f.plen)
				lats = append(lats, time.Since(f.sentAt))
			}
		}()
	}

	start := time.Now()
	for n := 0; n < p.messages; n++ {
		if pipelined && pubErr.Load() != nil {
			break
		}
		now := time.Now()
		if n > 0 && p.mps > 0 {
			next := start.Add(time.Duration(n) * time.Second / time.Duration(p.mps))
			time.Sleep(next.Sub(now))
		}

		payload := makePayload(n)
		currTopic := p.topic
		if p.topics > 0 {
			currTopic = p.topic + "/" + strconv.Itoa(iTopic)
			iTopic = (iTopic + 1) % p.topics
		}

		if pipelined {
			f := inflight{
				sentAt: time.Now(),
				plen:   mqttPublishLen(currTopic, byte(p.qos), p.retain, payload),
			}
			f.token = cl.Publish(currTopic, byte(p.qos), p.retain, payload)
			window <- f // blocks when the window is full
			continue
		}

		startPublish := time.Now()
		if token := cl.Publish(currTopic, byte(p.qos), p.retain, payload); !token.WaitTimeout(RunTimeout) {
			errorCh <- fmt.Errorf("timeout (%v) waiting for the ack", RunTimeout)
			return
		} else if token.Error() != nil {
			errorCh <- token.Error()
			return
		}
		elapsedPublish := time.Since(startPublish)
		elapsed += elapsedPublish
		lats = append(lats, elapsedPublish)
		logNoisy(opts.ClientID(), "PUB <-", elapsedPublish, "Published: %d bytes to %q, qos:%v, retain:%v", len(payload), currTopic, p.qos, p.retain)
		bc += mqttPublishLen(currTopic, byte(p.qos), p.retain, payload)
	}

	ops := p.messages
	bytes := int64(bc)
	if pipelined {
		close(window)
		<-collectorDone
		if errp := pubErr.Load(); errp != nil {
			// Blocking send, like the synchronous path: the run loop is
			// still waiting for this publisher and cannot miss it.
			errorCh <- *errp
			return
		}
		elapsed = time.Since(start)
		ops = int(okOps)
		bytes = okBytes
		logOp(opts.ClientID(), "PUB <-", elapsed, "Published (pipelined %d): %d messages, qos:%v, retain:%v", p.pipeline, ops, p.qos, p.retain)
	}

	if msgCh != nil {
		msgCh <- &Stat{
			Ops:   ops,
			NS:    map[string]time.Duration{"pub": elapsed},
			Bytes: bytes,
			Lat:   latencyStats(lats),
		}
	}
}
