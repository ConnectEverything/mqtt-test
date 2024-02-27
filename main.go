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
	"io"
	"log"
	"os"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nuid"
	"github.com/spf13/cobra"
)

const (
	Name                     = "mqtt-test"
	Version                  = "v0.2.0"
	DefaultServer            = "tcp://localhost:1883"
	DefaultQOS               = 0
	DisconnectCleanupTimeout = 2000 // milliseconds
)

var (
	ClientID string
	Password string
	Quiet    bool
	Servers  []string
	Username string
	Verbose  bool
	Timeout  time.Duration
)

type Stat struct {
	Ops   int           `json:"ops"`
	NS    time.Duration `json:"ns"`
	Bytes int64         `json:"bytes"`
}

func main() {
	_ = mainCmd.Execute()
	disconnectedWG.Wait()

	printTotals()
}

var mainCmd = &cobra.Command{
	Use:     Name + " [pub|sub|test...] [--flags...]",
	Short:   "MQTT Test and Benchmark Utility",
	Version: Version,
}

func init() {
	mainCmd.PersistentFlags().StringVar(&ClientID, "id", Name+"-"+nuid.Next(), "MQTT client ID")
	mainCmd.PersistentFlags().DurationVar(&Timeout, "timeout", 10*time.Second, "Timeout for the test")
	mainCmd.PersistentFlags().StringArrayVarP(&Servers, "server", "s", []string{DefaultServer}, "MQTT endpoint as username:password@host:port")
	mainCmd.PersistentFlags().BoolVarP(&Quiet, "quiet", "q", false, "Quiet mode, only print results")
	mainCmd.PersistentFlags().BoolVarP(&Verbose, "very-verbose", "v", false, "Very verbose, print everything we can")

	oldServers := mainCmd.PersistentFlags().StringArray("servers", nil, "MQTT endpoint as username:password@host:port")
	mainCmd.PersistentFlags().MarkDeprecated("servers", "please use server instead.")

	mainCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		paho.CRITICAL = log.New(os.Stderr, "[MQTT CRIT] ", 0)
		if Quiet {
			Verbose = false
			log.SetOutput(io.Discard)
		} else {
			paho.ERROR = log.New(os.Stderr, "[MQTT ERROR] ", 0)
		}
		if Verbose {
			paho.WARN = log.New(os.Stderr, "[MQTT WARN] ", 0)
			paho.DEBUG = log.New(os.Stderr, "[MQTT DEBUG] ", 0)
		}

		if len(*oldServers) > 0 {
			Servers = *oldServers
		}
	}

	mainCmd.AddCommand(newPubCommand())
	mainCmd.AddCommand(newPubSubCommand())
	mainCmd.AddCommand(newSubCommand())
	mainCmd.AddCommand(newSubRetCommand())
}

func defaultTopic() string { return Name + "/" + nuid.Next() }
