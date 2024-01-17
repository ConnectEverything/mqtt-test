package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/spf13/cobra"
)

const (
	Name    = "mqtt-test"
	Version = "v0.0.1"

	DefaultServer = "tcp://localhost:1883"
	DefaultQOS    = 0

	Timeout                  = 10 * time.Second
	DisconnectCleanupTimeout = 500 // milliseconds

)

var (
	ClientID         string
	MPS              int
	N                int
	NPublishers      int
	NSubscribers     int
	NTopics          int
	NTopicsStart     int
	Password         string
	QOS              int
	Retain           bool
	Servers          []string
	Size             int
	Username         string
	MatchTopicPrefix string
	Quiet            bool
	Verbose          bool
)

func initPubSub(cmd *cobra.Command) *cobra.Command {
	cmd.Flags().IntVar(&QOS, "qos", DefaultQOS, "MQTT QOS")
	cmd.Flags().IntVar(&Size, "size", 0, "Approximate size of each message (pub adds a timestamp)")
	return cmd
}

type PubValue struct {
	Seq       int   `json:"seq"`
	Timestamp int64 `json:"timestamp"`
}

type MQTTBenchmarkResult struct {
	Ops   int                      `json:"ops"`
	NS    map[string]time.Duration `json:"ns"`
	Bytes int64                    `json:"bytes"`
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

func mqttPublishLen(topic string, qos byte, retained bool, msg []byte) int {
	// Compute len (will have to add packet id if message is sent as QoS>=1)
	pkLen := 2 + len(topic) + len(msg)
	if qos > 0 {
		pkLen += 2
	}
	return 1 + mqttVarIntLen(pkLen) + pkLen
}

func logOp(clientID, op string, dur time.Duration, f string, args ...interface{}) {
	log.Printf("%8s %-6s %30s\t"+f, append([]any{
		fmt.Sprintf("%.3fms", float64(dur)/float64(time.Millisecond)),
		op,
		clientID + ":"},
		args...)...)
}
