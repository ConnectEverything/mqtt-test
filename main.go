package main

import (
	"io"
	"log"
	"os"
	"sync"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/cobra"
)

var mainCmd = &cobra.Command{
	Use:     Name + " [conn|pub|sub|...|] [--flags...]",
	Short:   "MQTT Test/Benchmark Utility",
	Version: Version,
}

func init() {
	mainCmd.PersistentFlags().StringVar(&ClientID, "id", "", "MQTT client ID")
	mainCmd.PersistentFlags().StringArrayVarP(&Servers, "servers", "s", []string{DefaultServer}, "MQTT servers endpoint as host:port")
	mainCmd.PersistentFlags().StringVarP(&Username, "username", "u", "", "MQTT client username (empty if auth disabled)")
	mainCmd.PersistentFlags().StringVarP(&Password, "password", "p", "", "MQTT client password (empty if auth disabled)")
	mainCmd.PersistentFlags().IntVarP(&N, "n", "n", 1, "Number of transactions to run, see the specific command")
	mainCmd.PersistentFlags().BoolVarP(&Quiet, "quiet", "q", false, "Quiet mode, only print results")
	mainCmd.PersistentFlags().BoolVarP(&Verbose, "very-verbose", "v", false, "Very verbose, print everything we can")

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
}

var disconnectedWG = sync.WaitGroup{}

func main() {
	_ = mainCmd.Execute()
	disconnectedWG.Wait()
}
