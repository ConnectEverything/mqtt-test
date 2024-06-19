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
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

type PubValue struct {
	Seq       int   `json:"seq"`
	Timestamp int64 `json:"timestamp"`
}

func randomPayload(sz int) []byte {
	const ch = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()"
	b := make([]byte, sz)
	for i := range b {
		b[i] = ch[rand.Intn(len(ch))]
	}
	return b
}

func mqttVarIntLen(value int) int {
	c := 0
	for ; value > 0; value >>= 7 {
		c++
	}
	return c
}

func mqttPublishLen(topic string, qos byte, retained bool, msg []byte) int64 {
	// Compute len (will have to add packet id if message is sent as QoS>=1)
	pkLen := 2 + len(topic) + len(msg)
	if qos > 0 {
		pkLen += 2
	}
	return int64(1 + mqttVarIntLen(pkLen) + pkLen)
}

type dial string

func (d dial) parse() (u, p, s, c string) {
	in := string(d)
	if in == "" {
		return "", "", DefaultServer, ""
	}

	if i := strings.LastIndex(in, "#"); i != -1 {
		c = in[i+1:]
		in = in[:i]
	}

	if i := strings.LastIndex(in, "@"); i != -1 {
		up := in[:i]
		in = in[i+1:]
		u = up
		if i := strings.Index(up, ":"); i != -1 {
			u = up[:i]
			p = up[i+1:]
		}
	}

	s = in
	return u, p, s, c
}

func (d dial) String() string {
	u, _, s, c := d.parse()
	if c != "" {
		return c
	}
	if u == "" {
		return s
	} else {
		return u + ":****@" + s
	}
}

var disconnectedWG = sync.WaitGroup{}

var Stats = make(map[string]*Stat)
var statsMu = new(sync.Mutex)

func recordOp(clientID string, d dial, name string, n int, dur time.Duration, bytes int64, f string, args ...any) {
	statsMu.Lock()
	stat, ok := Stats[name]
	if !ok {
		stat = new(Stat)
		Stats[name] = stat
	}
	stat.NS += dur
	stat.Ops += n
	stat.Bytes += bytes
	statsMu.Unlock()

	log.Printf("%8s %-6s %30s\t"+f, append([]any{
		fmt.Sprintf("%.3fms", float64(dur)/float64(time.Millisecond)),
		strings.ToUpper(name),
		clientID + "/" + d.String() + ":"},
		args...)...)
}

func printTotals() {
	statsMu.Lock()
	defer statsMu.Unlock()

	if len(Stats) == 0 {
		return
	}
	b, _ := json.MarshalIndent(Stats, "", "  ")
	os.Stdout.Write(b)
}

func dials(ss []string) []dial {
	d := make([]dial, 0, len(ss))
	for _, s := range ss {
		d = append(d, dial(s))
	}
	return d
}

func waitN(doneCh chan struct{}, N int, comment string) {
	if N == 0 {
		N = 1
	}
	for n := 0; n < N; n++ {
		select {
		case <-time.After(Timeout):
			log.Fatal("Error: timeout waiting for ", comment)
		case <-doneCh:
			// one is done
		}
	}
}
