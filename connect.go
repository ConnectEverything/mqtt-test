package main

import (
	"log"
	"strings"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nuid"
)

const (
	CleanSession      = true
	PersistentSession = false
)

var nextConnectServerIndex = atomic.Uint64{}

func connect(clientID string, cleanSession bool) (paho.Client, *Stat, func(), error) {
	if clientID == "" {
		clientID = ClientID
	}
	if clientID == "" {
		clientID = Name + "-" + nuid.Next()
	}

	parseDial := func(in string) (u, p, s, c string) {
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

	// round-robin the servers. since we start at 0 and add first, subtract 1 to
	// compensate and start at 0!
	next := int((nextConnectServerIndex.Add(1) - 1) % uint64(len(Servers)))
	u, p, s, c := parseDial(Servers[next])

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
		return nil, nil, nil, t.Error()
	}

	if c != "" {
		logOp(clientID, "CONN", time.Since(start), "Connected to %q (%s)\n", s, c)
	} else {
		logOp(clientID, "CONN", time.Since(start), "Connected to %q\n", s)
	}
	return cl,
		&Stat{
			Ops: 1,
			NS:  map[string]time.Duration{"conn": time.Since(start)},
		},
		func() {
			cl.Disconnect(DisconnectCleanupTimeout)
			disconnectedWG.Done()
		},
		nil
}
