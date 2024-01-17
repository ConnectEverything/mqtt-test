package main

import (
	"log"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nuid"
)

const (
	CleanSession      = true
	PersistentSession = false
)

func connect(clientID string, cleanSession bool, setoptsF func(*paho.ClientOptions)) (paho.Client, func(), error) {
	if clientID == "" {
		clientID = ClientID
	}
	if clientID == "" {
		clientID = Name + "-" + nuid.Next()
	}

	clientOpts := paho.NewClientOptions().
		SetClientID(clientID).
		SetCleanSession(cleanSession).
		SetProtocolVersion(4).
		SetUsername(Username).
		SetPassword(Password).
		SetStore(paho.NewMemoryStore()).
		SetAutoReconnect(false).
		SetDefaultPublishHandler(func(client paho.Client, msg paho.Message) {
			log.Fatalf("received an unexpected message on %q (default handler)", msg.Topic())
		})

	for _, s := range Servers {
		clientOpts.AddBroker(s)
	}
	if setoptsF != nil {
		setoptsF(clientOpts)
	}

	cl := paho.NewClient(clientOpts)

	disconnectedWG.Add(1)
	start := time.Now()
	if t := cl.Connect(); t.Wait() && t.Error() != nil {
		disconnectedWG.Done()
		return nil, func() {}, t.Error()
	}

	logOp(clientOpts.ClientID, "CONN", time.Since(start), "Connected to %q\n", Servers)
	return cl,
		func() {
			cl.Disconnect(DisconnectCleanupTimeout)
			disconnectedWG.Done()
		},
		nil
}
