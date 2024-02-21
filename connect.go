package main

import (
	"log"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nuid"
)

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

const (
	CleanSession      = true
	PersistentSession = false
)

func connect(d dial, clientID string, cleanSession bool) (paho.Client, func(), error) {
	if clientID == "" {
		clientID = ClientID
	}
	if clientID == "" {
		clientID = Name + "-" + nuid.Next()
	}
	u, p, s, _ := d.parse()

	cl := paho.NewClient(paho.NewClientOptions().
		SetClientID(clientID).
		SetCleanSession(cleanSession).
		SetProtocolVersion(4).
		AddBroker(s).
		SetUsername(u).
		SetPassword(p).
		SetStore(paho.NewMemoryStore()).
		SetAutoReconnect(false).
		SetDefaultPublishHandler(func(client paho.Client, msg paho.Message) {
			log.Fatalf("received an unexpected message on %q (default handler)", msg.Topic())
		}))

	disconnectedWG.Add(1)
	start := time.Now()
	if t := cl.Connect(); t.Wait() && t.Error() != nil {
		disconnectedWG.Done()
		return nil, nil, t.Error()
	}

	recordOp(clientID, d, "conn", 1, time.Since(start), 0, "Connected to %s\n", d.String())

	cleanup := func() {
		start := time.Now()
		cl.Disconnect(DisconnectCleanupTimeout)
		recordOp(clientID, d, "disc", 1, time.Since(start), 0, "Disconnected from %s\n", d.String())
		disconnectedWG.Done()
	}
	return cl, cleanup, nil
}
