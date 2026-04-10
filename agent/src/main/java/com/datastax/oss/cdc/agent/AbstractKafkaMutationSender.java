/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.cdc.agent;

import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.cdc.Constants;
import com.datastax.oss.cdc.KafkaMurmur3Partitioner;
import com.datastax.oss.cdc.MutationValue;
import com.datastax.oss.cdc.agent.exceptions.CassandraConnectorSchemaException;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kafka implementation of {@link MutationSender}.
 *
 * <p>A single {@link KafkaProducer} is shared across all topics. Each topic's Avro schemas
 * (key and value) are registered with a Confluent-compatible Schema Registry on first use.
 * Messages are serialized in the Confluent wire format:
 * {@code 0x00 | 4-byte-schema-id (big-endian) | avro-binary-payload}.
 *
 * <p>Cassandra partition tokens are propagated as a Kafka record header ({@code token}) so
 * consumers can implement their own token-aware routing if needed.
 */
@Slf4j
public abstract class AbstractKafkaMutationSender<T> implements MutationSender<T>, AutoCloseable {

    public static final String SCHEMA_DOC_PREFIX = "Primary key schema for table ";

    /** Confluent magic byte prepended to every schema-registry-encoded payload. */
    private static final byte CONFLUENT_MAGIC_BYTE = 0x00;

    static {
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlVarintConversion());
        SpecificData.get().addLogicalTypeConversion(new CqlLogicalTypes.CqlDecimalConversion());
        SpecificData.get().addLogicalTypeConversion(new Conversions.UUIDConversion());
    }

    /** Cached Avro schema + writer for a table's primary key. */
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class SchemaAndWriter {
        public final Schema schema;
        public final SpecificDatumWriter<GenericRecord> writer;
    }

    /** Cached schema-registry ID for a registered subject. */
    @AllArgsConstructor
    @ToString
    private static class RegisteredSchema {
        public final SchemaAndWriter schemaAndWriter;
        public final int schemaId;
    }

    volatile KafkaProducer<byte[], byte[]> producer;
    volatile SchemaRegistryClient schemaRegistryClient;

    /** key = topic name, value = registered key schema */
    final Map<String, RegisteredSchema> keySchemas = new ConcurrentHashMap<>();
    /** key = topic name, value = registered value schema */
    final Map<String, RegisteredSchema> valueSchemas = new ConcurrentHashMap<>();
    /** key = table key ("keyspace.table"), value = SchemaAndWriter for PK */
    final Map<String, SchemaAndWriter> pkSchemas = new ConcurrentHashMap<>();

    final AgentConfig config;
    final boolean useMurmur3Partitioner;

    /** Static Avro schema for {@link MutationValue} — built once at class-load time. */
    static final Schema MUTATION_VALUE_SCHEMA;

    static {
        MUTATION_VALUE_SCHEMA = ReflectData.get().getSchema(MutationValue.class);
    }

    public AbstractKafkaMutationSender(AgentConfig config, boolean useMurmur3Partitioner) {
        this.config = config;
        this.useMurmur3Partitioner = useMurmur3Partitioner;
    }

    public abstract Schema getNativeSchema(String cql3Type);
    public abstract Object cqlToAvro(T t, String columnName, Object value);
    public abstract boolean isSupported(AbstractMutation<T> mutation);
    public abstract void incSkippedMutations();
    public abstract UUID getHostId();

    /** Returns the topic name for the given table. */
    public String topicName(TableInfo tm) {
        return config.topicPrefix + tm.key();
    }

    @Override
    public void initialize(AgentConfig config) throws Exception {
        Map<String, Object> srConfig = new HashMap<>();
        if (config.schemaRegistryBasicAuthCredentialsSource != null) {
            srConfig.put("basic.auth.credentials.source", config.schemaRegistryBasicAuthCredentialsSource);
        }
        if (config.schemaRegistryBasicAuthUserInfo != null) {
            srConfig.put("basic.auth.user.info", config.schemaRegistryBasicAuthUserInfo);
        }
        if (config.schemaRegistrySslEnabled) {
            srConfig.put("schema.registry.ssl.truststore.location", config.sslTruststorePath);
            srConfig.put("schema.registry.ssl.truststore.password", config.sslTruststorePassword);
            srConfig.put("schema.registry.ssl.keystore.location", config.sslKeystorePath);
            srConfig.put("schema.registry.ssl.keystore.password", config.sslKeystorePassword);
        }
        this.schemaRegistryClient = new CachedSchemaRegistryClient(
                config.schemaRegistryUrl, 500, srConfig);
        log.info("Schema Registry client connected to {}", config.schemaRegistryUrl);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, config.kafkaAcks);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.kafkaBufferMemory);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.kafkaBatchSizeBytes);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.kafkaCompressionType);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, config.kafkaMaxBlockMs);
        if (config.kafkaLingerMs > 0) {
            props.put(ProducerConfig.LINGER_MS_CONFIG, config.kafkaLingerMs);
        }
        if (config.kafkaSecurityProtocol != null) {
            props.put("security.protocol", config.kafkaSecurityProtocol);
        }
        if (config.kafkaSaslMechanism != null) {
            props.put("sasl.mechanism", config.kafkaSaslMechanism);
        }
        if (config.kafkaSaslJaasConfig != null) {
            props.put("sasl.jaas.config", config.kafkaSaslJaasConfig);
        }
        if ("SSL".equals(config.kafkaSecurityProtocol) || "SASL_SSL".equals(config.kafkaSecurityProtocol)) {
            if (config.sslTruststorePath != null) {
                props.put("ssl.truststore.location", config.sslTruststorePath);
                props.put("ssl.truststore.password", config.sslTruststorePassword);
                props.put("ssl.truststore.type", config.sslTruststoreType);
            }
            if (config.sslKeystorePath != null) {
                props.put("ssl.keystore.location", config.sslKeystorePath);
                props.put("ssl.keystore.password", config.sslKeystorePassword);
            }
            if (config.sslEnabledProtocols != null) {
                props.put("ssl.enabled.protocols", config.sslEnabledProtocols);
            }
            if (config.sslCipherSuites != null) {
                props.put("ssl.cipher.suites", config.sslCipherSuites);
            }
        }
        if (useMurmur3Partitioner) {
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    "com.datastax.oss.cdc.KafkaMurmur3Partitioner");
        }
        this.producer = new KafkaProducer<>(props);
        log.info("Kafka producer connected to {}", config.kafkaBootstrapServers);

        if (config.kafkaTopicAutoCreate) {
            // AdminClient is created and closed — used here only to validate connectivity
            @SuppressWarnings("try")
            AdminClient adminClient = AdminClient.create(props);
            adminClient.close();
            log.info("Kafka AdminClient ready for topic auto-creation");
        }
    }

    /**
     * Builds the Avro schema for the primary key of the given table, caching the result.
     */
    public SchemaAndWriter getAvroKeySchema(final TableInfo tableInfo) {
        return pkSchemas.computeIfAbsent(tableInfo.key(), k -> {
            List<Schema.Field> fields = new ArrayList<>();
            for (ColumnInfo cm : tableInfo.primaryKeyColumns()) {
                Schema.Field field = new Schema.Field(cm.name(), getNativeSchema(cm.cql3Type()));
                if (cm.isClusteringKey()) {
                    field = new Schema.Field(cm.name(),
                            Schema.createUnion(Schema.create(Schema.Type.NULL), field.schema()));
                }
                fields.add(field);
            }
            Schema avroSchema = Schema.createRecord(
                    tableInfo.key(), SCHEMA_DOC_PREFIX + tableInfo.key(),
                    tableInfo.name(), false, fields);
            return new SchemaAndWriter(avroSchema, new SpecificDatumWriter<>(avroSchema));
        });
    }

    /**
     * Builds an Avro {@link GenericRecord} containing the mutation's primary key values.
     */
    public GenericRecord buildAvroKey(Schema keySchema, AbstractMutation<T> mutation) {
        org.apache.avro.generic.GenericData.Record genericRecord =
                new org.apache.avro.generic.GenericData.Record(keySchema);
        int i = 0;
        for (ColumnInfo columnInfo : mutation.primaryKeyColumns()) {
            if (keySchema.getField(columnInfo.name()) == null) {
                throw new CassandraConnectorSchemaException(
                        "Not a valid schema field: " + columnInfo.name());
            }
            genericRecord.put(columnInfo.name(),
                    cqlToAvro(mutation.getMetadata(), columnInfo.name(), mutation.getPkValues()[i++]));
        }
        return genericRecord;
    }

    /**
     * Serializes an Avro {@link GenericRecord} to binary Avro bytes (no schema ID prefix).
     */
    public byte[] serializeAvroGenericRecord(
            GenericRecord genericRecord,
            SpecificDatumWriter<GenericRecord> datumWriter) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = new EncoderFactory().binaryEncoder(out, null);
            datumWriter.write(genericRecord, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Serializes an Avro generic record to Confluent wire format:
     * {@code 0x00 | 4-byte schema ID (big-endian) | avro binary payload}.
     */
    private byte[] toConfluentWireFormat(int schemaId, byte[] avroBytes) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + avroBytes.length);
        buf.put(CONFLUENT_MAGIC_BYTE);
        buf.putInt(schemaId);
        buf.put(avroBytes);
        return buf.array();
    }

    /**
     * Gets or registers the key schema for the given topic and returns the registered schema info.
     */
    private RegisteredSchema getOrRegisterKeySchema(TableInfo tableInfo) {
        String topic = topicName(tableInfo);
        return keySchemas.computeIfAbsent(topic, k -> {
            SchemaAndWriter saw = getAvroKeySchema(tableInfo);
            String subject = topic + "-key";
            try {
                int id = config.schemaRegistryAutoRegister
                        ? schemaRegistryClient.register(subject, new AvroSchema(saw.schema))
                        : schemaRegistryClient.getId(subject, new AvroSchema(saw.schema));
                log.info("Key schema registered for subject={} schemaId={}", subject, id);
                return new RegisteredSchema(saw, id);
            } catch (Exception e) {
                throw new RuntimeException("Failed to register key schema for subject " + subject, e);
            }
        });
    }

    /**
     * Gets or registers the value schema (MutationValue) for the given topic.
     */
    private RegisteredSchema getOrRegisterValueSchema(TableInfo tableInfo) {
        String topic = topicName(tableInfo);
        return valueSchemas.computeIfAbsent(topic, k -> {
            String subject = topic + "-value";
            SpecificDatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(MUTATION_VALUE_SCHEMA);
            try {
                int id = config.schemaRegistryAutoRegister
                        ? schemaRegistryClient.register(subject, new AvroSchema(MUTATION_VALUE_SCHEMA))
                        : schemaRegistryClient.getId(subject, new AvroSchema(MUTATION_VALUE_SCHEMA));
                log.info("Value schema registered for subject={} schemaId={}", subject, id);
                return new RegisteredSchema(new SchemaAndWriter(MUTATION_VALUE_SCHEMA, writer), id);
            } catch (Exception e) {
                throw new RuntimeException("Failed to register value schema for subject " + subject, e);
            }
        });
    }

    /**
     * Serializes a MutationValue as an Avro GenericRecord using reflection.
     */
    private GenericRecord mutationValueToGenericRecord(MutationValue mv) {
        org.apache.avro.generic.GenericData.Record record =
                new org.apache.avro.generic.GenericData.Record(MUTATION_VALUE_SCHEMA);
        record.put("md5Digest", mv.getMd5Digest());
        record.put("nodeId", mv.getNodeId() != null ? mv.getNodeId().toString() : null);
        if (mv.getColumns() != null) {
            record.put("columns", Arrays.asList(mv.getColumns()));
        } else {
            record.put("columns", null);
        }
        return record;
    }

    private void ensureInitialized() throws Exception {
        if (producer == null) {
            synchronized (this) {
                if (producer == null) {
                    initialize(config);
                }
            }
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public CompletableFuture<?> sendMutationAsync(final AbstractMutation<T> mutation) {
        if (!isSupported(mutation)) {
            incSkippedMutations();
            return CompletableFuture.completedFuture(null);
        }
        try {
            ensureInitialized();

            RegisteredSchema keyReg = getOrRegisterKeySchema(mutation);
            RegisteredSchema valueReg = getOrRegisterValueSchema(mutation);

            byte[] keyAvro = serializeAvroGenericRecord(
                    buildAvroKey(keyReg.schemaAndWriter.schema, mutation),
                    keyReg.schemaAndWriter.writer);
            byte[] keyBytes = toConfluentWireFormat(keyReg.schemaId, keyAvro);

            MutationValue mv = mutation.mutationValue();
            byte[] valueAvro = serializeAvroGenericRecord(
                    mutationValueToGenericRecord(mv),
                    valueReg.schemaAndWriter.writer);
            byte[] valueBytes = toConfluentWireFormat(valueReg.schemaId, valueAvro);

            String topic = topicName(mutation);
            RecordHeaders headers = new RecordHeaders();
            headers.add(Constants.SEGMENT_AND_POSITION,
                    (mutation.getSegment() + ":" + mutation.getPosition())
                            .getBytes(StandardCharsets.UTF_8));
            headers.add(Constants.TOKEN,
                    mutation.getToken().toString().getBytes(StandardCharsets.UTF_8));
            if (mutation.getTs() != -1) {
                headers.add(Constants.WRITETIME,
                        String.valueOf(mutation.getTs()).getBytes(StandardCharsets.UTF_8));
            }

            // Set the Cassandra token in a ThreadLocal so KafkaMurmur3Partitioner can read it
            KafkaMurmur3Partitioner.TOKEN_HOLDER.set(mutation.getToken());

            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(topic, null, null, keyBytes, valueBytes, headers);

            CompletableFuture<Object> future = new CompletableFuture<>();
            producer.send(record, (metadata, exception) -> {
                KafkaMurmur3Partitioner.TOKEN_HOLDER.remove();
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(metadata);
                }
            });
            return future;
        } catch (Exception e) {
            CompletableFuture future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            synchronized (this) {
                if (producer != null) {
                    producer.close();
                    producer = null;
                }
            }
        }
    }
}
