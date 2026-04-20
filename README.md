# DataStax CDC for Apache Cassandra

[![CI](https://github.com/datastax/cdc-apache-cassandra/actions/workflows/ci.yaml/badge.svg)](https://github.com/datastax/cdc-apache-cassandra/actions/workflows/ci.yaml)
![documentation](https://github.com/datastax/cdc-apache-cassandra/actions/workflows/publish.yml/badge.svg)
![release](https://github.com/datastax/cdc-apache-cassandra/actions/workflows/release.yaml/badge.svg)
[![GitHub release](https://img.shields.io/github/v/release/datastax/cdc-apache-cassandra.svg)](https://github.com/datastax/cdc-apache-cassandra/releases/latest)

The DataStax CDC for Apache Cassandra requires:

* DataStax Change Agent for Apache Cassandra, which is an event producer deployed as a JVM agent on each Cassandra data node.
* A streaming platform sink: either the DataStax Source Connector for Apache Pulsar, or the built-in Kafka producer included in the Kafka agent jars.

![Cassandra-source-connector](./docs/modules/ROOT/assets/images/cassandra-source-connector.png)

Supported streaming platforms:
* Apache Pulsar 2.8.1+
* IBM Elite Support for Apache Pulsar (formerly DataStax Luna Streaming) 2.8.0.1.1.40+
* Apache Kafka (any broker compatible with the Kafka client 3.x API)

Supported Cassandra version:
* Cassandra 3.11+
* Cassandra 4.0+
* [DataStax Enterprise (DSE)](https://www.datastax.com/products/datastax-enterprise) 6.8.16+
* [DataStax Hyper-Converged Database (HCD)](https://docs.datastax.com/en/hyper-converged-database/1.2/get-started/hcd-introduction.html) 1.2.3+

Note: Only Cassandra 4.0, DSE 6.8.16+, and HCD 1.2.3+ support the near realtime CDC allowing to replicate data as soon as they are synced on disk.

## Documentation

To get started, see [QUICKSTART.md](QUICKSTART.md).

For the complete documentation, see the [CDC for Apache Cassandra documentation](https://docs.datastax.com/en/cdc-for-cassandra/docs/latest/index.html).

## Demo

Cassandra data replicated to Elasticsearch:

* Create a Cassandra table with cdc enabled
* Deploy a Cassandra source and an Elasticsearch sink into Apache Pulsar
* Writes into Cassandra are replicated to Elasticsearch.

[![asciicast](https://asciinema.org/a/kiEYzHQrPWhJR19nZ7tbqrDIX.png)](https://asciinema.org/a/kiEYzHQrPWhJR19nZ7tbqrDIX?speed=2&theme=tango)

## Monitoring

You can collect Cassandra/DSE and Pulsar metrics into Prometheus, and build a Grafana dashboard with:
* The CQL read latency from the Cassandra Source Connector
* The replication latency from the Cassandra Source Connector (computed from the Cassandra writetime)
* The CDC disk space used in the cdc_raw directory (for DSE only)
* The mutation sent throughput from a Cassandra node
* The pulsar events and data topic rate in

![CDC Dashboard](./docs/modules/ROOT/assets/images/cdc-dashboard.png)

## Limitations

* Does not replay logged batches
* Does not manage table truncates
* Does not manage TTLs
* Does not support range deletes
* Does not sync data available before starting the CDC agent.
* CQL column names must not match a [Pulsar primitive type](https://pulsar.apache.org/docs/next/schema-understand/#primitive-type) name (ex: INT32)
* Does not support primary key only tables (ex: CREATE TABLE t (k int, c int, PRIMARY KEY (k, c)) WITH cdc=true;)

## Supported data types

Cassandra supported CQL3 data types (with the associated AVRO type or logical-type):

* text (string), ascii (string)
* tinyint (int), smallint (int), int (int), bigint (long), double (double), float (float),
* inet (string)
* decimal (cql_decimal), varint (cql_varint), duration (cql_duration)
* blob(bytes)
* boolean (boolean)
* timestamp (timestamp-millis), time (time-micros), date (date)
* uuid, timeuuid (uuid)
* User Defined Types (record)
* tuple (record)
* Collection types:
** list (array)
** set (array)
** map (map)

## Pulsar vs Kafka: single-stage and two-stage architectures

### Pulsar — two-stage pipeline

The Pulsar integration uses a two-stage pipeline:

1. **Stage 1 (agent → events topic):** The Change Agent reads the Cassandra commit log and publishes a lightweight event containing only the primary key columns to the *events topic*.
2. **Stage 2 (connector → data topic):** The DataStax Source Connector for Pulsar consumes the events topic, performs a CQL read-back to fetch the full row from Cassandra, and publishes the complete row to the *data topic*.

**Topic naming (Pulsar)**

| Topic | Default name pattern | Example |
|---|---|---|
| Events topic | `events-<keyspace>.<table>` | `events-ks1.orders` |
| Data topic | `data-<keyspace>.<table>` | `data-ks1.orders` |

The `topicPrefix` agent parameter (env var `CDC_TOPIC_PREFIX`, default `events-`) controls the events topic prefix.  The data topic prefix is always `data-` and is fixed by the connector.

### Kafka — single-stage pipeline

The Kafka integration collapses both stages into one. The Change Agent reads the commit log, fetches the current full row from Cassandra internally (using `QueryProcessor.executeInternal`), and publishes the complete Avro-encoded row directly to a single Kafka topic. No separate connector process is required.

* **INSERT / UPDATE** — message key is the Avro-encoded primary key; message value is the full Avro-encoded row.
* **DELETE** — message key is the Avro-encoded primary key; message value is `null` (Kafka tombstone) with an `op=DELETE` record header.

**Topic naming (Kafka)**

| Topic | Default name pattern | Example |
|---|---|---|
| Data topic | `<topicPrefix><keyspace>.<table>` | `events-ks1.orders` |

The same `CDC_TOPIC_PREFIX` environment variable (default `events-`) controls the Kafka topic prefix.  Because there is only one topic per table, consumers receive the full row on every change event.

Avro schemas for both the key (primary key columns) and value (all columns) are registered with a Confluent-compatible Schema Registry on first use and are cached per topic thereafter.

## Build from the sources

### Pulsar agents

    # Cassandra 4.x / OSS (default)
    ./gradlew assemble

    # DSE 6.8+
    ./gradlew assemble -Pdse4

    # HCD 1.2.3+
    ./gradlew assemble -Phcd

### Kafka agents

The Kafka agent modules are built with the `shadowJar` task, which produces a self-contained fat jar.  The `agent-c4-kafka` module is always included; the DSE and HCD variants require their respective property flags (same as above).

    # Cassandra 4.x / OSS
    ./gradlew :agent-c4-kafka:shadowJar

    # DSE 6.8+
    ./gradlew :agent-dse4-kafka:shadowJar -Pdse4

    # HCD 1.2.3+
    ./gradlew :agent-hcd-kafka:shadowJar -Phcd

Output jars land in the respective module's `build/libs/` directory, e.g. `agent-c4-kafka/build/libs/agent-c4-kafka-<version>-all.jar`.

## Kafka agent environment variables

The following environment variables are read at agent startup to configure the Kafka producer and Schema Registry client.  They can also be passed as inline agent parameters (e.g., `-javaagent:agent.jar=kafkaBootstrapServers=broker:9092,...`).

### General (shared with Pulsar agents)

| Environment variable | Parameter name | Default | Description |
|---|---|---|---|
| `CDC_TOPIC_PREFIX` | `topicPrefix` | `events-` | Topic name prefix; `<keyspace>.<table>` is appended to form the full topic name. |
| `CDC_WORKING_DIR` | `cdcWorkingDir` | `<cassandra.storagedir>/cdc` | Working directory for offset tracking and commitlog archiving. |
| `CDC_DIR_POLL_INTERVAL_MS` | `cdcPollIntervalMs` | `60000` | Poll interval (ms) for new commitlog files in the CDC raw directory. |
| `CDC_CONCURRENT_PROCESSORS` | `cdcConcurrentProcessors` | `-1` (= `memtable_flush_writers`) | Thread count for commitlog processing. |
| `CDC_ERROR_COMMITLOG_REPROCESS_ENABLED` | `errorCommitLogReprocessEnabled` | `false` | Re-process commitlog files that previously errored. |

### Kafka broker

| Environment variable | Parameter name | Default | Description |
|---|---|---|---|
| `CDC_KAFKA_BOOTSTRAP_SERVERS` | `kafkaBootstrapServers` | `localhost:9092` | Comma-separated list of Kafka broker addresses (`host:port`). |
| `CDC_KAFKA_SECURITY_PROTOCOL` | `kafkaSecurityProtocol` | `PLAINTEXT` | Security protocol: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`. |
| `CDC_KAFKA_SASL_MECHANISM` | `kafkaSaslMechanism` | _(none)_ | SASL mechanism, e.g. `PLAIN`, `SCRAM-SHA-256`, `OAUTHBEARER`. |
| `CDC_KAFKA_SASL_JAAS_CONFIG` | `kafkaSaslJaasConfig` | _(none)_ | Full JAAS config string for SASL authentication. |
| `CDC_KAFKA_ACKS` | `kafkaAcks` | `all` | Producer acknowledgement mode: `0`, `1`, or `all`. |
| `CDC_KAFKA_LINGER_MS` | `kafkaLingerMs` | `-1` (disabled) | Producer batching linger time (ms). Batching is disabled when ≤ 0. |
| `CDC_KAFKA_BATCH_SIZE_BYTES` | `kafkaBatchSizeBytes` | `16384` | Maximum bytes per producer batch. |
| `CDC_KAFKA_BUFFER_MEMORY` | `kafkaBufferMemory` | `33554432` | Total producer buffer memory in bytes. |
| `CDC_KAFKA_COMPRESSION_TYPE` | `kafkaCompressionType` | `none` | Compression type: `none`, `gzip`, `snappy`, `lz4`, or `zstd`. |
| `CDC_KAFKA_MAX_BLOCK_MS` | `kafkaMaxBlockMs` | `60000` | Max time (ms) the producer blocks when the buffer is full. |
| `CDC_KAFKA_TOPIC_AUTO_CREATE` | `kafkaTopicAutoCreate` | `false` | Automatically create topics via AdminClient before first produce. |
| `CDC_KAFKA_TOPIC_REPLICATION_FACTOR` | `kafkaTopicReplicationFactor` | `1` | Replication factor used when auto-creating topics. |

### Schema Registry

| Environment variable | Parameter name | Default | Description |
|---|---|---|---|
| `CDC_SCHEMA_REGISTRY_URL` | `schemaRegistryUrl` | `http://localhost:8081` | URL(s) of the Confluent-compatible Avro Schema Registry. |
| `CDC_SR_AUTO_REGISTER` | `schemaRegistryAutoRegister` | `true` | Automatically register new Avro schemas on first use. |
| `CDC_SR_BASIC_AUTH_CREDENTIALS_SOURCE` | `schemaRegistryBasicAuthCredentialsSource` | _(none)_ | Credential source for Schema Registry basic auth: `USER_INFO` or `URL`. |
| `CDC_SR_BASIC_AUTH_USER_INFO` | `schemaRegistryBasicAuthUserInfo` | _(none)_ | Schema Registry credentials in `user:password` format. |
| `CDC_SR_SSL_ENABLED` | `schemaRegistrySslEnabled` | `false` | Use HTTPS for Schema Registry connections (reuses `sslTruststorePath` / `sslKeystorePath`). |

### TLS / SSL (shared with Pulsar agents)

| Environment variable | Parameter name | Default | Description |
|---|---|---|---|
| `CDC_SSL_TRUSTSTORE_PATH` | `sslTruststorePath` | _(none)_ | Path to the TLS truststore file. |
| `CDC_SSL_TRUSTSTORE_PASSWORD` | `sslTruststorePassword` | _(none)_ | Truststore password. |
| `CDC_SSL_TRUSTSTORE_TYPE` | `sslTruststoreType` | `JKS` | Truststore type. |
| `CDC_SSL_KEYSTORE_PATH` | `sslKeystorePath` | _(none)_ | Path to the TLS keystore file. |
| `CDC_SSL_KEYSTORE_PASSWORD` | `sslKeystorePassword` | _(none)_ | Keystore password. |
| `CDC_SSL_ENABLED_PROTOCOLS` | `sslEnabledProtocols` | `TLSv1.2,TLSv1.1,TLSv1` | Enabled TLS protocol versions. |
| `CDC_SSL_CIPHER_SUITES` | `sslCipherSuites` | _(none)_ | Comma-separated list of allowed cipher suites. |
| `CDC_TLS_TRUST_CERTS_FILE_PATH` | `tlsTrustCertsFilePath` | _(none)_ | Path to a PEM-encoded trusted certificate file. |
| `CDC_USE_KEYSTORE_TLS` | `useKeyStoreTls` | `false` | Use keystore-based TLS instead of PEM files. |

## Acknowledgments

Apache Cassandra, Apache Pulsar, Cassandra and Pulsar are trademarks of the Apache Software Foundation.
Elasticsearch, is a trademark of Elasticsearch BV, registered in the U.S. and in other countries.
