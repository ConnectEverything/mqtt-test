MQTT Test is a CLI command used to test and benchmark the MQTT support in [NATS Server](https://github.com/nats-io/nats-server)

Outputs JSON results that can be reported in a `go test --bench` wrapper.

#### Usage

##### Subcommands and common flags

```sh
mqtt-test [pub|sub|pubsub|subret] [flags...]
```

Available Commands:
- [pub](#pub) - Publish MQTT messages.
- [sub](#sub) - Subscribe, receive all messages, unsubscribe, {repeat} times.
- [pubsub](#pubsub) - Subscribe and receive published messages.
- [subret](#subret) - Publish {topics} retained messages, subscribe {repeat} times, and receive all retained messages.

Common flags:
```
  -h, --help                 help for mqtt-test
      --id string            MQTT client ID (default "mqtt-test-bssJjZUs1vhTvf6KpTpTLw")
  -q, --quiet                Quiet mode, only print results
  -s, --server stringArray   MQTT endpoint as username:password@host:port (default [tcp://localhost:1883])
      --version              version for mqtt-test
  -v, --very-verbose         Very verbose, print everything we can
```

##### pub

Publishes messages using the flags and reports the results. Used with `--publishers` can run several concurrent publish connections.

Flags:

```
--messages int     Number of transactions to run, see the specific command (default 1)
--mps int          Publish mps messages per second; 0 means no delay (default 1000)
--publishers int   Number of publishers to run concurrently, at --mps each (default 1)
--qos int          MQTT QOS
--retain           Mark each published message as retained
--size int         Approximate size of each message (pub adds a timestamp)
--timestamp        Prepend a timestamp to each message
--topic string     Base topic (prefix) to publish into (/{n} will be added if --topics > 0) (default "mqtt-test/fIqfOq5Lg5wk636V4sLXoc")
--topics int       Cycle through NTopics appending "/{n}"
```

##### sub

Subscribe, receive all expected messages, unsubscribe, {repeat} times.

Flags:

```
--messages int      Expect to receive this many published messages
--qos int           MQTT QOS
--repeat int        Subscribe, receive retained messages, and unsubscribe N times (default 1)
--retained int      Expect to receive this many retained messages
--subscribers int   Number of subscribers to run concurrently (default 1)
--topic string      Base topic for the test, will subscribe to {topic}/+
```

##### pubsub

Publishes N messages, and waits for all of them to be received by subscribers. Measures end-end delivery time on the messages. Used with `--num-subscribers` can run several concurrent subscriber connections.

```
--messages int      Number of messages to publish and receive (default 1)
--qos int           MQTT QOS
--size int          Approximate size of each message (pub adds a timestamp)
--subscribers int   Number of subscribers to run concurrently (default 1)
--topic string      Topic to publish and subscribe to (default "mqtt-test/JPrbNU6U3IbVQLIyazkP4y")
```

##### subret

Publishes retained messages into NTopics, then subscribes to a wildcard with all
topics N times. Measures time to SUBACK and to all retained messages received.
Used with `--subscribers` can run several concurrent subscriber connections.

```
--qos int           MQTT QOS
--repeat int        Subscribe, receive retained messages, and unsubscribe N times (default 1)
--size int          Approximate size of each message (pub adds a timestamp)
--subscribers int   Number of subscribers to run concurrently (default 1)
--topic string      Base topic (prefix) for the test (default "mqtt-test/yNkmAFnFHETSGnQJNjwGdN")
--topics int        Number of sub-topics to publish retained messages to (default 1)
```
