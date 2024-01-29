package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nuid"
	"github.com/spf13/cobra"
)

const (
	Name                     = "mqtt-test"
	Version                  = "v0.1.0"
	DefaultServer            = "tcp://localhost:1883"
	DefaultQOS               = 0
	Timeout                  = 10 * time.Second
	DisconnectCleanupTimeout = 500 // milliseconds
)

var (
	ClientID string
	Password string
	Quiet    bool
	Servers  []string
	Username string
	Verbose  bool
)

var disconnectedWG = sync.WaitGroup{}

func main() {
	_ = mainCmd.Execute()
	disconnectedWG.Wait()
}

var mainCmd = &cobra.Command{
	Use:     Name + " [pub|sub|subret|...] [--flags...]",
	Short:   "MQTT Test and Benchmark Utility",
	Version: Version,
}

func init() {
	mainCmd.PersistentFlags().StringVar(&ClientID, "id", Name+"-"+nuid.Next(), "MQTT client ID")
	mainCmd.PersistentFlags().StringArrayVarP(&Servers, "server", "s", []string{DefaultServer}, "MQTT endpoint as username:password@host:port")
	mainCmd.PersistentFlags().BoolVarP(&Quiet, "quiet", "q", false, "Quiet mode, only print results")
	mainCmd.PersistentFlags().BoolVarP(&Verbose, "very-verbose", "v", false, "Very verbose, print everything we can")

	mainCmd.PersistentFlags().StringArrayVar(&Servers, "servers", []string{DefaultServer}, "MQTT endpoint as username:password@host:port")
	mainCmd.PersistentFlags().MarkDeprecated("servers", "please use server instead.")

	mainCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		paho.CRITICAL = log.New(os.Stderr, "[MQTT CRIT] ", 0)
		if Quiet {
			Verbose = false
			log.SetOutput(io.Discard)
		}
		if !Quiet {
			paho.ERROR = log.New(os.Stderr, "[MQTT ERROR] ", 0)
		}
		if Verbose {
			paho.WARN = log.New(os.Stderr, "[MQTT WARN] ", 0)
			paho.DEBUG = log.New(os.Stderr, "[MQTT DEBUG] ", 0)
		}
	}

	mainCmd.AddCommand(newPubCommand())
	mainCmd.AddCommand(newPubSubCommand())
	mainCmd.AddCommand(newSubCommand())
	mainCmd.AddCommand(newSubRetCommand())
}

type PubValue struct {
	Seq       int   `json:"seq"`
	Timestamp int64 `json:"timestamp"`
}

type Stat struct {
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

func defaultTopic() string { return Name + "/" + nuid.Next() }

func logOp(clientID, op string, dur time.Duration, f string, args ...interface{}) {
	log.Printf("%8s %-6s %30s\t"+f, append([]any{
		fmt.Sprintf("%.3fms", float64(dur)/float64(time.Millisecond)),
		op,
		clientID + ":"},
		args...)...)
}
