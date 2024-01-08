MQTT Test is a CLI command used to test and benchmark the MQTT support in [NATS Server](https://github.com/nats-io/nats-server)

Outputs JSON results that can be reported in a `go test --bench` wrapper.

#### Usage

##### Subcommands and common flags

```sh
mqtt-test [pub|pubsub|subret] [flags...]
```

Available Commands:
- [pub](#pub) - Publish N messages
- [pubsub](#pubsub) - Subscribe and receive N published messages
- [subret](#subret) - Subscribe N times, and receive NTopics retained messages
Common Flags:
```
-h, --help                  help for mqtt-test
    --id string             MQTT client ID
-n, --n int                 Number of transactions to run, see the specific command (default 1)
-p, --password string       MQTT client password (empty if auth disabled)
-q, --quiet                 Quiet mode, only print results
-s, --servers stringArray   MQTT servers endpoint as host:port (default [tcp://localhost:1883])
-u, --username string       MQTT client username (empty if auth disabled)
    --version               version for mqtt-test
-v, --very-verbose          Very verbose, print everything we can
```

##### pub

Publishes N messages using the flags and reports the results. Used with `--num-publishers` can run several concurrent publish connections.

Flags:

```
--mps int                Publish mps messages per second; 0 means no delay (default 1000)
--num-publishers int     Number of publishers to run concurrently, at --mps each (default 1)
--num-topics int         Cycle through NTopics appending "-{n}" where n starts with --num-topics-start; 0 means use --topic
--num-topics-start int   Start topic suffixes with this number (default 0)
--qos int                MQTT QOS
--retain                 Mark each published message as retained
--size int               Approximate size of each message (pub adds a timestamp)
--topic string           MQTT topic
```

##### pubsub

Publishes N messages, and waits for all of them to be received by subscribers. Measures end-end delivery time on the messages. Used with `--num-subscribers` can run several concurrent subscriber connections.

```
--mps int               Publish mps messages per second; 0 means all ASAP (default 1000)
--num-subscribers int   Number of subscribers to run concurrently (default 1)
--qos int               MQTT QOS
--size int              Approximate size of each message (pub adds a timestamp)
--topic string          MQTT topic
```

##### subret

Publishes retained messages into NTopics, then subscribes to a wildcard with all
topics N times. Measures time to SUBACK and to all retained messages received.
Used with `--num-subscribers` can run several concurrent subscriber connections.

```
--num-subscribers int   Number of subscribers to run concurrently (default 1)
--num-topics int        Use this many topics with retained messages
--qos int               MQTT QOS
--size int              Approximate size of each message (pub adds a timestamp)
```
