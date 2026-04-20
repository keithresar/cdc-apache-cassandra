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
import com.datastax.oss.cdc.agent.exceptions.CassandraConnectorSchemaException;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Kafka-backed {@link MutationSender} for DataStax Enterprise 4.x.
 *
 * <p>Implements the single-stage pattern: reads the commit log, fetches the current full row
 * from Cassandra via internal query APIs, and publishes the complete Avro-encoded row to Kafka.
 * DELETE mutations produce a Kafka tombstone (null value) with an {@code op=DELETE} header.
 */
@Slf4j
public class KafkaMutationSender extends AbstractKafkaMutationSender<TableMetadata> {

    private static final ImmutableMap<String, Schema> AVRO_SCHEMA_TYPES =
            ImmutableMap.<String, Schema>builder()
                    .put(UTF8Type.instance.asCQL3Type().toString(),
                            Schema.create(Schema.Type.STRING))
                    .put(AsciiType.instance.asCQL3Type().toString(),
                            Schema.create(Schema.Type.STRING))
                    .put(BooleanType.instance.asCQL3Type().toString(),
                            Schema.create(Schema.Type.BOOLEAN))
                    .put(BytesType.instance.asCQL3Type().toString(),
                            Schema.create(Schema.Type.BYTES))
                    .put(ByteType.instance.asCQL3Type().toString(),
                            Schema.create(Schema.Type.INT))
                    .put(ShortType.instance.asCQL3Type().toString(),
                            Schema.create(Schema.Type.INT))
                    .put(Int32Type.instance.asCQL3Type().toString(),
                            Schema.create(Schema.Type.INT))
                    .put(IntegerType.instance.asCQL3Type().toString(), CqlLogicalTypes.varintType)
                    .put(LongType.instance.asCQL3Type().toString(),
                            Schema.create(Schema.Type.LONG))
                    .put(FloatType.instance.asCQL3Type().toString(),
                            Schema.create(Schema.Type.FLOAT))
                    .put(DoubleType.instance.asCQL3Type().toString(),
                            Schema.create(Schema.Type.DOUBLE))
                    .put(DecimalType.instance.asCQL3Type().toString(), CqlLogicalTypes.decimalType)
                    .put(InetAddressType.instance.asCQL3Type().toString(),
                            Schema.create(Schema.Type.STRING))
                    .put(TimestampType.instance.asCQL3Type().toString(), CqlLogicalTypes.timestampMillisType)
                    .put(SimpleDateType.instance.asCQL3Type().toString(), CqlLogicalTypes.dateType)
                    .put(TimeType.instance.asCQL3Type().toString(), CqlLogicalTypes.timeMicrosType)
                    .put(DurationType.instance.asCQL3Type().toString(), CqlLogicalTypes.durationType)
                    .put(UUIDType.instance.asCQL3Type().toString(), CqlLogicalTypes.uuidType)
                    .put(TimeUUIDType.instance.asCQL3Type().toString(), CqlLogicalTypes.uuidType)
                    .build();

    public KafkaMutationSender(AgentConfig config) {
        super(config, DatabaseDescriptor.getPartitionerName().equals(Murmur3Partitioner.class.getName()));
    }

    public KafkaMutationSender(AgentConfig config, boolean useMurmur3Partitioner) {
        super(config, useMurmur3Partitioner);
    }

    @Override
    public void incSkippedMutations() {
        CdcMetrics.skippedMutations.inc();
    }

    @Override
    public UUID getHostId() {
        return StorageService.instance.getLocalHostUUID();
    }

    @Override
    public Schema getNativeSchema(String cql3Type) {
        return AVRO_SCHEMA_TYPES.get(cql3Type);
    }

    @Override
    public boolean isSupported(final AbstractMutation<TableMetadata> mutation) {
        if (!pkSchemas.containsKey(mutation.key())) {
            for (ColumnMetadata cm : mutation.metadata.primaryKeyColumns()) {
                if (!AVRO_SCHEMA_TYPES.containsKey(cm.type.asCQL3Type().toString())) {
                    log.warn("Unsupported primary key column={}.{}.{} type={}, skipping mutation",
                            cm.ksName, cm.cfName, cm.name, cm.type.asCQL3Type().toString());
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Object cqlToAvro(TableMetadata tableMetadata, String columnName, Object value) {
        ColumnMetadata columnMetadata = tableMetadata.getColumn(
                ColumnIdentifier.getInterned(columnName, false));
        AbstractType<?> type = columnMetadata.type.isReversed()
                ? ((ReversedType<?>) columnMetadata.type).baseType
                : columnMetadata.type;
        return convertToAvro(type, null, value);
    }

    // -------------------------------------------------------------------------
    // Full-row schema building
    // -------------------------------------------------------------------------

    @Override
    public SchemaAndWriter buildRowAvroSchema(AbstractMutation<TableMetadata> mutation) {
        TableMetadata tm = mutation.metadata;
        List<Schema.Field> fields = new ArrayList<>();

        Iterator<ColumnMetadata> colIt = tm.allColumnsInSelectOrder();
        while (colIt.hasNext()) {
            ColumnMetadata cm = colIt.next();
            AbstractType<?> rawType = cm.type.isReversed()
                    ? ((ReversedType<?>) cm.type).baseType : cm.type;
            Schema fieldSchema = buildColumnSchema(rawType);
            if (!cm.isPartitionKey()) {
                fieldSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), fieldSchema);
            }
            fields.add(new Schema.Field(cm.name.toString(), fieldSchema));
        }

        Schema avroSchema = Schema.createRecord(
                mutation.key(), "Full row schema for " + mutation.key(),
                mutation.name(), false, fields);
        return new SchemaAndWriter(avroSchema, new SpecificDatumWriter<>(avroSchema));
    }

    Schema buildColumnSchema(AbstractType<?> type) {
        if (type instanceof ReversedType) {
            type = ((ReversedType<?>) type).baseType;
        }

        Schema primitive = AVRO_SCHEMA_TYPES.get(type.asCQL3Type().toString());
        if (primitive != null) return primitive;

        if (type instanceof ListType<?>) {
            return Schema.createArray(buildColumnSchema(((ListType<?>) type).getElementsType()));
        }
        if (type instanceof SetType<?>) {
            return Schema.createArray(buildColumnSchema(((SetType<?>) type).getElementsType()));
        }
        if (type instanceof MapType<?, ?>) {
            return Schema.createMap(buildColumnSchema(((MapType<?, ?>) type).getValuesType()));
        }
        if (type instanceof UserType) {
            return buildUdtSchema((UserType) type);
        }
        if (type instanceof TupleType) {
            return buildTupleSchema((TupleType) type);
        }

        throw new CassandraConnectorSchemaException(
                "Unsupported column type for Avro schema: " + type.asCQL3Type());
    }

    private Schema buildUdtSchema(UserType udt) {
        String fullName = (udt.keyspace + "_" + udt.getNameAsString()).replace('.', '_');
        List<Schema.Field> fields = new ArrayList<>();
        for (int i = 0; i < udt.size(); i++) {
            Schema fs = Schema.createUnion(
                    Schema.create(Schema.Type.NULL),
                    buildColumnSchema(udt.type(i)));
            fields.add(new Schema.Field(udt.fieldName(i).toString(), fs));
        }
        return Schema.createRecord(fullName, "UDT " + udt.getNameAsString(), udt.keyspace, false, fields);
    }

    private Schema buildTupleSchema(TupleType tuple) {
        String name = "tuple_" + Math.abs(tuple.toString().hashCode());
        List<Schema.Field> fields = new ArrayList<>();
        for (int i = 0; i < tuple.size(); i++) {
            Schema fs = Schema.createUnion(
                    Schema.create(Schema.Type.NULL),
                    buildColumnSchema(tuple.type(i)));
            fields.add(new Schema.Field("field" + i, fs));
        }
        return Schema.createRecord(name, "Tuple", "", false, fields);
    }

    // -------------------------------------------------------------------------
    // Row fetching
    // -------------------------------------------------------------------------

    @Override
    public Optional<GenericRecord> fetchAndBuildRowRecord(
            Schema schema, AbstractMutation<TableMetadata> mutation) {

        if (mutation.getOp() == MutationType.DELETE) {
            return Optional.empty();
        }

        TableMetadata tm = mutation.metadata;

        StringBuilder cql = new StringBuilder("SELECT * FROM ")
                .append('"').append(tm.keyspace).append('"')
                .append('.')
                .append('"').append(tm.name).append('"')
                .append(" WHERE ");
        List<Object> params = new ArrayList<>();
        boolean first = true;
        int pkIdx = 0;
        for (ColumnMetadata cm : tm.partitionKeyColumns()) {
            if (!first) cql.append(" AND ");
            cql.append('"').append(cm.name).append('"').append("=?");
            params.add(mutation.getPkValues()[pkIdx++]);
            first = false;
        }
        for (ColumnMetadata cm : tm.clusteringColumns()) {
            if (pkIdx >= mutation.getPkValues().length) break;
            Object val = mutation.getPkValues()[pkIdx++];
            if (val != null) {
                cql.append(" AND ").append('"').append(cm.name).append('"').append("=?");
                params.add(val);
            }
        }

        UntypedResultSet rs;
        try {
            rs = QueryProcessor.executeInternal(cql.toString(), params.toArray());
        } catch (Exception e) {
            log.warn("Failed to fetch row for mutation key={}: {}", mutation.key(), e.getMessage());
            return Optional.empty();
        }

        if (rs == null || rs.isEmpty()) {
            return Optional.empty();
        }

        UntypedResultSet.Row row = rs.one();
        GenericRecord record = new GenericData.Record(schema);

        Iterator<ColumnMetadata> colIt = tm.allColumnsInSelectOrder();
        while (colIt.hasNext()) {
            ColumnMetadata cm = colIt.next();
            String name = cm.name.toString();
            Schema fieldSchema = schema.getField(name).schema();
            Schema actualSchema = fieldSchema.getType() == Schema.Type.UNION
                    ? fieldSchema.getTypes().get(1)
                    : fieldSchema;

            AbstractType<?> type = cm.type.isReversed()
                    ? ((ReversedType<?>) cm.type).baseType : cm.type;

            if (!row.has(name)) {
                record.put(name, null);
                continue;
            }

            ByteBuffer raw = row.getBlob(name);
            if (raw == null || !raw.hasRemaining()) {
                record.put(name, null);
                continue;
            }

            Object composed = type.compose(raw.duplicate());
            record.put(name, convertToAvro(type, actualSchema, composed));
        }

        return Optional.of(record);
    }

    // -------------------------------------------------------------------------
    // CQL → Avro value conversion
    // -------------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "rawtypes"})
    Object convertToAvro(AbstractType<?> type, Schema avroSchema, Object value) {
        if (value == null) return null;

        if (type instanceof ReversedType) {
            type = ((ReversedType<?>) type).baseType;
        }

        if (type instanceof TimestampType) {
            if (value instanceof Date) return ((Date) value).getTime();
            if (value instanceof Instant) return ((Instant) value).toEpochMilli();
        }
        if (type instanceof SimpleDateType && value instanceof Integer) {
            long timeInMillis = Duration.ofDays((Integer) value + Integer.MIN_VALUE).toMillis();
            Instant instant = Instant.ofEpochMilli(timeInMillis);
            LocalDate localDate = LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalDate();
            return (int) localDate.toEpochDay();
        }
        if (type instanceof TimeType && value instanceof Long) {
            return ((Long) value / 1000);
        }
        if (type instanceof InetAddressType) {
            return ((InetAddress) value).getHostAddress();
        }
        if (type instanceof ByteType) {
            return Byte.toUnsignedInt((byte) value);
        }
        if (type instanceof ShortType) {
            return Short.toUnsignedInt((short) value);
        }

        if (type instanceof ListType<?> || type instanceof SetType<?>) {
            AbstractType<?> elementType = (type instanceof ListType<?>)
                    ? ((ListType<?>) type).getElementsType()
                    : ((SetType<?>) type).getElementsType();
            Collection<?> collection = (Collection<?>) value;
            Schema elementSchema = avroSchema != null ? avroSchema.getElementType() : null;
            List<Object> avroList = new ArrayList<>(collection.size());
            for (Object elem : collection) {
                avroList.add(convertToAvro(elementType, elementSchema, elem));
            }
            return avroSchema != null
                    ? new GenericData.Array<>(avroSchema, avroList)
                    : avroList;
        }
        if (type instanceof MapType<?, ?>) {
            MapType<?, ?> mt = (MapType<?, ?>) type;
            Map<?, ?> map = (Map<?, ?>) value;
            Schema valueSchema = avroSchema != null ? avroSchema.getValueType() : null;
            Map<String, Object> avroMap = new LinkedHashMap<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = entry.getKey() != null ? entry.getKey().toString() : "null";
                avroMap.put(key, convertToAvro(mt.getValuesType(), valueSchema, entry.getValue()));
            }
            return avroMap;
        }

        if (type instanceof UserType) {
            UserType udt = (UserType) type;
            ByteBuffer rawUdt = (ByteBuffer) value;
            ByteBuffer[] fieldBufs = udt.split(rawUdt);
            Schema udtSchema = avroSchema != null ? avroSchema : buildUdtSchema(udt);
            GenericData.Record udtRecord = new GenericData.Record(udtSchema);
            for (int i = 0; i < udt.size(); i++) {
                String fieldName = udt.fieldName(i).toString();
                if (i < fieldBufs.length && fieldBufs[i] != null && fieldBufs[i].hasRemaining()) {
                    AbstractType<?> fieldType = udt.type(i);
                    Schema fieldSchema = getUnionInnerSchema(udtSchema, fieldName);
                    Object fieldVal = fieldType.compose(fieldBufs[i].duplicate());
                    udtRecord.put(fieldName, convertToAvro(fieldType, fieldSchema, fieldVal));
                } else {
                    udtRecord.put(fieldName, null);
                }
            }
            return udtRecord;
        }

        if (type instanceof TupleType) {
            TupleType tuple = (TupleType) type;
            ByteBuffer rawTuple = (ByteBuffer) value;
            ByteBuffer[] fieldBufs = tuple.split(rawTuple);
            Schema tupleSchema = avroSchema != null ? avroSchema : buildTupleSchema(tuple);
            GenericData.Record tupleRecord = new GenericData.Record(tupleSchema);
            for (int i = 0; i < tuple.size(); i++) {
                String fieldName = "field" + i;
                if (i < fieldBufs.length && fieldBufs[i] != null && fieldBufs[i].hasRemaining()) {
                    AbstractType<?> fieldType = tuple.type(i);
                    Schema fieldSchema = getUnionInnerSchema(tupleSchema, fieldName);
                    Object fieldVal = fieldType.compose(fieldBufs[i].duplicate());
                    tupleRecord.put(fieldName, convertToAvro(fieldType, fieldSchema, fieldVal));
                } else {
                    tupleRecord.put(fieldName, null);
                }
            }
            return tupleRecord;
        }

        return value;
    }

    private static Schema getUnionInnerSchema(Schema recordSchema, String fieldName) {
        Schema.Field field = recordSchema.getField(fieldName);
        if (field == null) return null;
        Schema fs = field.schema();
        return fs.getType() == Schema.Type.UNION ? fs.getTypes().get(1) : fs;
    }
}
