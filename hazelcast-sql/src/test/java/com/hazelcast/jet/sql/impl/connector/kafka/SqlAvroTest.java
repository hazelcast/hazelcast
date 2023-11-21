/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple4;
import com.hazelcast.jet.kafka.HazelcastKafkaAvroDeserializer;
import com.hazelcast.jet.kafka.HazelcastKafkaAvroSerializer;
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple4.tuple4;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_AVRO_RECORD_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.kafka.SqlAvroSchemaEvolutionTest.NAME_SSN_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver.Schemas.OBJECT_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver.optional;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.copyOfRange;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class SqlAvroTest extends KafkaSqlTestSupport {
    private static final int INITIAL_PARTITION_COUNT = 4;

    static final Schema ID_SCHEMA = SchemaBuilder.record("jet.sql")
            .fields()
            .optionalInt("id")
            .endRecord();
    static final Schema NAME_SCHEMA = SchemaBuilder.record("jet.sql")
            .fields()
            .optionalString("name")
            .endRecord();
    static final Schema ALL_TYPES_SCHEMA = SchemaBuilder.record("jet.sql")
            .fields()
            .optionalString("string")
            .optionalBoolean("boolean")
            .optionalInt("byte")
            .optionalInt("short")
            .optionalInt("int")
            .optionalLong("long")
            .optionalFloat("float")
            .optionalDouble("double")
            .optionalString("decimal")
            .optionalString("time")
            .optionalString("date")
            .optionalString("timestamp")
            .optionalString("timestampTz")
            .name("map").type(OBJECT_SCHEMA).withDefault(null)
            .name("object").type(OBJECT_SCHEMA).withDefault(null)
            .endRecord();

    @Parameters(name = "useSchemaRegistry=[{0}]")
    public static Iterable<Object> parameters() {
        return List.of(false, true);
    }

    @Parameter
    public boolean useSchemaRegistry;

    private Type mapping;
    private Schema keySchema;
    private Schema valueSchema;
    private Map<String, String> clientProperties;

    @BeforeClass
    public static void setup() throws Exception {
        setup(1, null);
        createSchemaRegistry();
    }

    private SqlMapping kafkaMapping(String name) {
        return kafkaMapping(name, null, null);
    }

    private SqlMapping kafkaMapping(String name, Schema keySchema, Schema valueSchema) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        clientProperties = useSchemaRegistry
                ? ImmutableMap.of("schema.registry.url", kafkaTestSupport.getSchemaRegistryURI().toString())
                : ImmutableMap.of(OPTION_KEY_AVRO_SCHEMA, keySchema.toString(),
                                  OPTION_VALUE_AVRO_SCHEMA, valueSchema.toString());
        kafkaTestSupport.setProducerProperties(name, clientProperties);

        return new KafkaMapping(name)
                .options(OPTION_KEY_FORMAT, AVRO_FORMAT,
                         OPTION_VALUE_FORMAT, AVRO_FORMAT,
                         "bootstrap.servers", kafkaTestSupport.getBrokerConnectionString(),
                         "auto.offset.reset", "earliest")
                .optionsIf(useSchemaRegistry,
                           "schema.registry.url", kafkaTestSupport.getSchemaRegistryURI())
                .optionsIf(!useSchemaRegistry,
                           OPTION_KEY_AVRO_SCHEMA, keySchema,
                           OPTION_VALUE_AVRO_SCHEMA, valueSchema);
    }

    @Test
    public void test_nestedField() {
        String name = createRandomTopic();
        new SqlType("Organization").fields("id INT", "name VARCHAR").create();

        Schema orgSchema = SchemaBuilder.record("Organization").fields()
                .optionalInt("id")
                .optionalString("name")
                .endRecord();
        Schema personSchema = SchemaBuilder.record("Person").fields()
                .optionalString("name")
                .name("organization").type().optional().type(orgSchema)
                .endRecord();

        kafkaMapping(name, ID_SCHEMA, personSchema)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR",
                        "organization Organization")
                .optionsIf(useSchemaRegistry,
                           OPTION_VALUE_AVRO_RECORD_NAME, "Person")
                .create();

        insertAndAssertRecord(
                row(1, "Alice", row(1, "Umbrella Corporation")),
                row(1, "Alice", row(1, "Umbrella Corporation")),
                row(1, "Alice", createRecord(orgSchema, 1, "Umbrella Corporation")));

        assertRowsEventuallyInAnyOrder(
                "SELECT name, (organization).name FROM " + name,
                List.of(new Row("Alice", "Umbrella Corporation"))
        );
    }

    @Test
    public void when_mappingOrTypeSchemaHasMissingField_then_fail() {
        assumeFalse(useSchemaRegistry);
        new SqlType("Parent").fields("name VARCHAR", "phone VARCHAR").create();

        Schema parentSchema = SchemaBuilder.record("Parent").fields()
                .requiredString("name")
                .endRecord();
        Schema studentSchema = SchemaBuilder.record("Student").fields()
                .requiredString("name")
                .name("parent").type(parentSchema).noDefault()
                .endRecord();

        assertThatThrownBy(() ->
                kafkaMapping("kafka", ID_SCHEMA, studentSchema)
                        .fields("id INT EXTERNAL NAME \"__key.id\"",
                                "name VARCHAR",
                                "address VARCHAR",
                                "parent Parent")
                        .create())
                .hasMessage("Field 'address' does not exist in schema");

        assertThatThrownBy(() ->
                kafkaMapping("kafka", ID_SCHEMA, studentSchema)
                        .fields("id INT EXTERNAL NAME \"__key.id\"",
                                "name VARCHAR",
                                "parent Parent")
                        .create())
                .hasMessage("Field 'phone' does not exist in schema");
    }

    @Test
    public void test_mappingAndTypeHasMissingOptionalField() {
        assumeFalse(useSchemaRegistry);
        String name = createRandomTopic();
        new SqlType("Parent").fields("name VARCHAR", "phone VARCHAR").create();

        Schema parentSchema = SchemaBuilder.record("Parent").fields()
                .requiredString("name")
                .name("address").type().stringType().stringDefault("<unknown>")
                .requiredString("phone")
                .endRecord();
        Schema studentSchema = SchemaBuilder.record("Student").fields()
                .requiredString("name")
                .optionalLong("ssn")
                .name("parent").type(parentSchema).noDefault()
                .endRecord();

        kafkaMapping(name, ID_SCHEMA, studentSchema)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR",
                        "parent Parent")
                .create();

        insertAndAssertRecord(
                row(1, "Alice", row("Bob", "(111) 111-1111")),
                row(1, "Alice", row("Bob", "(111) 111-1111")),
                row(1, "Alice", createRecord(parentSchema, "Bob", "<unknown>", "(111) 111-1111")));

        kafkaTestSupport.produce(name, createRecord(ID_SCHEMA, 3),
                createRecord(studentSchema, "Dave", 123456789L,
                        row("Erin", "Insignificant St. 34", "(999) 999-9999")));

        assertRowsEventuallyInAnyOrder(
                "SELECT name, (parent).name, (parent).phone FROM " + name,
                List.of(
                        new Row("Alice", "Bob", "(111) 111-1111"),
                        new Row("Dave", "Erin", "(999) 999-9999")
                )
        );
    }

    @Test
    public void when_mappingOrTypeHasMissingMandatoryField_then_fail() {
        assumeFalse(useSchemaRegistry);
        new SqlType("Parent").fields("name VARCHAR").create();

        Schema parentSchema = SchemaBuilder.record("Parent").fields()
                .requiredString("name")
                .requiredString("phone")
                .endRecord();
        Schema studentSchema = SchemaBuilder.record("Student").fields()
                .requiredString("name")
                .name("parent").type(parentSchema).noDefault()
                .endRecord();

        assertThatThrownBy(() ->
                kafkaMapping("kafka", ID_SCHEMA, studentSchema)
                        .fields("id INT EXTERNAL NAME \"__key.id\"",
                                "name VARCHAR")
                        .create())
                .hasMessage("Mandatory field 'parent' is not mapped to any column");

        assertThatThrownBy(() ->
                kafkaMapping("kafka", ID_SCHEMA, studentSchema)
                        .fields("id INT EXTERNAL NAME \"__key.id\"",
                                "name VARCHAR",
                                "parent Parent")
                        .create())
                .hasMessage("Mandatory field 'phone' is not mapped to any column");
    }

    @Test
    public void test_mappingHasDifferentFieldOrder() {
        assumeFalse(useSchemaRegistry);
        String name = createRandomTopic();
        kafkaMapping(name, ID_SCHEMA, NAME_SSN_SCHEMA)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "ssn BIGINT",
                        "name VARCHAR")
                .create();

        insertAndAssertRecord(1, 123456789L, "Alice");
    }

    @Test
    public void test_nonNullField() {
        assumeFalse(useSchemaRegistry);
        String name = createRandomTopic();
        Schema schema = SchemaBuilder.record("jet.sql").fields()
                .requiredString("name")
                .endRecord();

        kafkaMapping(name, ID_SCHEMA, schema)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        insertAndAssertRecord(1, "Alice");
        assertThatThrownBy(() -> insertRecord(2, null))
                .hasMessageContaining("Field name type:STRING pos:0 does not accept null values");
    }

    @Test
    public void when_valueCannotBeConverted_then_fail() {
        assumeFalse(useSchemaRegistry);
        String name = createRandomTopic();
        kafkaMapping(name, ID_SCHEMA, NAME_SCHEMA)
                .fields("id BIGINT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        assertThatThrownBy(() -> insertRecord(Long.MAX_VALUE, "Alice"))
                .hasMessageContaining("Cannot convert " + Long.MAX_VALUE + " to INT (field=id)");
    }

    @Test
    public void test_unionWithString() {
        assumeFalse(useSchemaRegistry);
        String name = createRandomTopic();
        Schema schema = SchemaBuilder.record("jet.sql").fields()
                .optionalString("ssn")
                .name("info").type().unionOf().nullType().and().booleanType().and().stringType().endUnion().nullDefault()
                .endRecord();

        kafkaMapping(name, ID_SCHEMA, schema)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "ssn INT",
                        "info OBJECT")
                .create();

        // Regardless of whether the target Avro type is nullable primitive or union,
        // as long as it contains (or is) String, the input is converted into String.
        // When reading back, if the column doesn't imply a conversion, like in the
        // case of OBJECT, the converted value is returned as-is, so 42 becomes "42".
        insertAndAssertRecord(row(1, 123456789, 42), row(1, "123456789", "42"), row(1, 123456789, "42"));
    }

    @Test
    public void test_nonInclusiveUnion() {
        assumeFalse(useSchemaRegistry);
        String name = createRandomTopic();
        Schema schema = SchemaBuilder.record("jet.sql").fields()
                .name("info").type().unionOf().nullType().and().booleanType().and().intType().endUnion().nullDefault()
                .endRecord();

        kafkaMapping(name, ID_SCHEMA, schema)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "info OBJECT")
                .create();

        insertRecord(1, null);
        insertRecord(2, true);
        insertRecord(3, "true");
        insertRecord(4, 42);
        insertRecord(5, "42");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(
                        new Row(1, null),
                        new Row(2, true),
                        new Row(3, true),
                        new Row(4, 42),
                        new Row(5, 42)
                )
        );

        assertThatThrownBy(() -> insertRecord(6, Long.MAX_VALUE)).hasMessageContaining(
                "Not in union [\"null\",\"boolean\",\"int\"]: " + Long.MAX_VALUE + " (Long) (field=info)");
    }

    @Test
    public void test_fieldResolution() {
        assumeFalse(useSchemaRegistry);

        // Test field resolution for top-level fields
        Schema childSchema = SchemaBuilder.record("child").fields()
                .requiredBoolean("requiredBoolean")
                .nullableBoolean("nullableBoolean", true)
                .optionalBoolean("optionalBoolean")
                .requiredInt("requiredInt")
                .nullableInt("nullableInt", 1)
                .optionalInt("optionalInt")
                .requiredLong("requiredLong")
                .nullableLong("nullableLong", 1L)
                .optionalLong("optionalLong")
                .requiredFloat("requiredFloat")
                .nullableFloat("nullableFloat", 1F)
                .optionalFloat("optionalFloat")
                .requiredDouble("requiredDouble")
                .nullableDouble("nullableDouble", 1D)
                .optionalDouble("optionalDouble")
                .requiredString("requiredString")
                .nullableString("nullableString", "A")
                .optionalString("optionalString")
                .name("requiredUnion").type().unionOf().intType().and().stringType().endUnion().noDefault()
                .name("nullableUnion").type().unionOf().intType().and().stringType().and().nullType().endUnion()
                        .intDefault(1)
                .name("optionalUnion").type().unionOf().nullType().and().intType().and().stringType().endUnion()
                        .nullDefault()
                .endRecord();

        String name = createRandomTopic();
        kafkaMapping(name, ID_SCHEMA, childSchema).create();

        new SqlInsert(name)
                .literals("id", 1,
                          "requiredBoolean", false,
                          "requiredInt", 0,
                          "requiredLong", 0L,
                          "requiredFloat", 0F,
                          "requiredDouble", 0D,
                          "requiredString", "",
                          "requiredUnion", 0)
                // TODO Remove nullable fields after introducing RexUnset
                .literals("nullableBoolean", true,
                          "nullableInt", 1,
                          "nullableLong", 1L,
                          "nullableFloat", 1F,
                          "nullableDouble", 1D,
                          "nullableString", "A",
                          "nullableUnion", 1)
                .execute();

        Object[] allRow = {
                1,
                false, true, null,
                0, 1, null,
                0L, 1L, null,
                0F, 1F, null,
                0D, 1D, null,
                "", "A", null,
                0, 1, null
        };
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(allRow))
        );

        // Test field resolution for nested fields
        Schema parentSchema = SchemaBuilder.record("parent").fields()
                .name("requiredChild").type(childSchema).noDefault()
                .name("nullableChild").type(SchemaBuilder.nullable().type(childSchema))
                        .withDefault(new GenericRecordBuilder(childSchema)
                                .set("requiredBoolean", false)
                                .set("requiredInt", 0)
                                .set("requiredLong", 0L)
                                .set("requiredFloat", 0F)
                                .set("requiredDouble", 0D)
                                .set("requiredString", "")
                                .set("requiredUnion", 0)
                                .build())
                .name("optionalChild").type().optional().type(childSchema)
                .endRecord();

        new SqlType("Child").create();
        name = createRandomTopic();
        kafkaMapping(name, ID_SCHEMA, parentSchema)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "requiredChild Child",
                        "nullableChild Child",
                        "optionalChild Child")
                .create();

        Object[] valueRow = copyOfRange(allRow, 1, allRow.length);
        new SqlInsert(name)
                .literals("id", 1,
                          "requiredChild", valueRow)
                // TODO Remove nullableChild after introducing RexUnset
                .literals("nullableChild", valueRow)
                .execute();

        GenericRecord record = createRecord(childSchema, valueRow);
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(new Row(1, record, record, null))
        );
    }

    @Test
    public void test_allConversions() {
        assumeFalse(useSchemaRegistry);

        // Test all QueryDataType <-> Schema.Type conversions
        List<Tuple4<QueryDataTypeFamily, Object, Schema.Type, Object>> conversions = new ArrayList<>();
        conversions.addAll(cartesian(
                List.of(tuple2(QueryDataTypeFamily.BOOLEAN, true)),
                List.of(tuple2(Schema.Type.BOOLEAN, true),
                        tuple2(Schema.Type.STRING, "true"))));
        conversions.addAll(cartesian(
                List.of(tuple2(QueryDataTypeFamily.TINYINT, (byte) 1),
                        tuple2(QueryDataTypeFamily.SMALLINT, (short) 1),
                        tuple2(QueryDataTypeFamily.INTEGER, 1),
                        tuple2(QueryDataTypeFamily.BIGINT, 1L),
                        tuple2(QueryDataTypeFamily.REAL, 1F),
                        tuple2(QueryDataTypeFamily.DOUBLE, 1D),
                        tuple2(QueryDataTypeFamily.DECIMAL, BigDecimal.ONE)),
                List.of(tuple2(Schema.Type.INT, 1),
                        tuple2(Schema.Type.LONG, 1L),
                        tuple2(Schema.Type.FLOAT, 1F),
                        tuple2(Schema.Type.DOUBLE, 1D))));
        conversions.addAll(cartesian(
                List.of(tuple2(QueryDataTypeFamily.TINYINT, (byte) 1),
                        tuple2(QueryDataTypeFamily.SMALLINT, (short) 1),
                        tuple2(QueryDataTypeFamily.INTEGER, 1),
                        tuple2(QueryDataTypeFamily.BIGINT, 1L),
                        tuple2(QueryDataTypeFamily.DECIMAL, BigDecimal.ONE)),
                List.of(tuple2(Schema.Type.STRING, "1"))));
        conversions.addAll(cartesian(
                List.of(tuple2(QueryDataTypeFamily.REAL, 1F),
                        tuple2(QueryDataTypeFamily.DOUBLE, 1D)),
                List.of(tuple2(Schema.Type.STRING, "1.0"))));
        conversions.addAll(List.of(
                tuple4(QueryDataTypeFamily.TIME, LocalTime.of(12, 23, 34), Schema.Type.STRING, "12:23:34"),
                tuple4(QueryDataTypeFamily.DATE, LocalDate.of(2020, 4, 15), Schema.Type.STRING, "2020-04-15"),
                tuple4(QueryDataTypeFamily.TIMESTAMP, LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
                        Schema.Type.STRING, "2020-04-15T12:23:34.001"),
                tuple4(QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE,
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                        Schema.Type.STRING, "2020-04-15T12:23:34.200Z")));
        conversions.addAll(cartesian(
                List.of(tuple2(QueryDataTypeFamily.VARCHAR, "1")),
                List.of(tuple2(Schema.Type.INT, 1),
                        tuple2(Schema.Type.LONG, 1L),
                        tuple2(Schema.Type.STRING, "1"))));
        conversions.addAll(cartesian(
                List.of(tuple2(QueryDataTypeFamily.VARCHAR, "1.0")),
                List.of(tuple2(Schema.Type.FLOAT, 1F),
                        tuple2(Schema.Type.DOUBLE, 1D))));
        conversions.add(tuple4(QueryDataTypeFamily.VARCHAR, "true", Schema.Type.BOOLEAN, true));
        conversions.add(tuple4(QueryDataTypeFamily.OBJECT, "string", Schema.Type.UNION, "string"));
        conversions.add(tuple4(QueryDataTypeFamily.OBJECT, null, Schema.Type.NULL, null));

        Map<QueryDataType, String> mappingFieldTypes = ImmutableMap.<QueryDataType, String>builder()
                .put(QueryDataType.BOOLEAN, "BOOLEAN")
                .put(QueryDataType.TINYINT, "TINYINT")
                .put(QueryDataType.SMALLINT, "SMALLINT")
                .put(QueryDataType.INT, "INT")
                .put(QueryDataType.BIGINT, "BIGINT")
                .put(QueryDataType.REAL, "REAL")
                .put(QueryDataType.DOUBLE, "DOUBLE")
                .put(QueryDataType.DECIMAL, "DECIMAL")
                .put(QueryDataType.VARCHAR, "VARCHAR")
                .put(QueryDataType.TIME, "TIME")
                .put(QueryDataType.DATE, "DATE")
                .put(QueryDataType.TIMESTAMP, "TIMESTAMP")
                .put(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "TIMESTAMP WITH TIME ZONE")
                .put(QueryDataType.OBJECT, "OBJECT")
                .put(QueryDataType.JSON, "JSON")
                .build();

        List<Schema> schemaFieldTypes = new ArrayList<>();
        Stream.of(
                Schema.Type.BOOLEAN,
                Schema.Type.INT,
                Schema.Type.LONG,
                Schema.Type.FLOAT,
                Schema.Type.DOUBLE,
                Schema.Type.STRING,
                Schema.Type.NULL,
                Schema.Type.BYTES
        ).forEach(type -> schemaFieldTypes.add(Schema.create(type)));
        schemaFieldTypes.addAll(List.of(
                OBJECT_SCHEMA, // Schema.Type.UNION
                SchemaBuilder.array().items(Schema.create(Schema.Type.INT)),
                SchemaBuilder.map().values(Schema.create(Schema.Type.INT)),
                SchemaBuilder.enumeration("enum").symbols("symbol"),
                SchemaBuilder.fixed("fixed").size(0),
                SchemaBuilder.record("record").fields().endRecord()));

        for (Entry<QueryDataType, String> entry : mappingFieldTypes.entrySet()) {
            QueryDataType mappingFieldType = entry.getKey();
            QueryDataTypeFamily mappingFieldTypeFamily = mappingFieldType.getTypeFamily();
            String sqlFieldType = entry.getValue();

            boolean mappingFieldTypeSupported = conversions.stream().anyMatch(c -> mappingFieldTypeFamily == c.f0());

            for (Schema fieldSchema : schemaFieldTypes) {
                Schema.Type schemaFieldType = fieldSchema.getType();
                Schema valueSchema = SchemaBuilder.record("jet.sql").fields()
                        .name("info").type(optional(fieldSchema)).withDefault(null)
                        .endRecord();
                Consumer<String> createKafkaMapping = name ->
                        kafkaMapping(name, ID_SCHEMA, valueSchema)
                                .fields("id INT EXTERNAL NAME \"__key.id\"",
                                        "info " + sqlFieldType)
                                .create();

                if (mappingFieldTypeSupported) {
                    Tuple2<Object, Object> conversion = conversions.stream()
                            .filter(c -> mappingFieldTypeFamily == c.f0() && schemaFieldType == c.f2())
                            .map(c -> tuple2(c.f1(), c.f3())).findFirst().orElse(null);
                    if (conversion != null) {
                        String name = createRandomTopic();
                        createKafkaMapping.accept(name);

                        System.out.println(">> " + mappingFieldType + " <- " + conversion.f1() + ":" + schemaFieldType);
                        insertAndAssertRecord(row(1, conversion.f0()), row(1, conversion.f1()));
                    } else {
                        assertThatThrownBy(() -> createKafkaMapping.accept("kafka"))
                                .hasMessage(schemaFieldType + " schema type is incompatible with "
                                        + mappingFieldType + " mapping type");
                    }
                } else {
                    assertThatThrownBy(() -> createKafkaMapping.accept("kafka"))
                            .hasMessage("Unsupported type: " + mappingFieldType);
                }
            }
        }
    }

    @Test
    public void test_nulls() {
        String name = createRandomTopic();
        kafkaMapping(name, ID_SCHEMA, NAME_SCHEMA)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        insertAndAssertRecord((Integer) null, null);
    }

    @Test
    public void test_fieldsMapping() {
        String name = createRandomTopic();
        kafkaMapping(name, NAME_SCHEMA, NAME_SCHEMA)
                .fields("key_name VARCHAR EXTERNAL NAME \"__key.name\"",
                        "value_name VARCHAR EXTERNAL NAME \"this.name\"")
                .create();

        insertAndAssertRecord("Alice", "Bob");
    }

    @Test
    public void test_schemaEvolution() {
        String name = createRandomTopic();
        kafkaMapping(name, ID_SCHEMA, NAME_SCHEMA)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        // insert initial record
        insertRecord(13, "Alice");

        // alter schema
        kafkaMapping(name, ID_SCHEMA, NAME_SSN_SCHEMA)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR",
                        "ssn BIGINT")
                .createOrReplace();

        // insert record against new schema
        insertRecord(69, "Bob", 123456789);

        // assert both - initial & evolved - records are correctly read
        Runnable assertRecords = () -> assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + name,
                List.of(
                        new Row(13, "Alice", null),
                        new Row(69, "Bob", 123456789L)
                )
        );
        if (useSchemaRegistry) {
            assertRecords.run();
        } else {
            assertThatThrownBy(assertRecords::run)
                    .hasMessageContaining("Error deserializing key/value");
        }
    }

    @Test
    public void test_allTypes() {
        String from = randomName();
        TestAllTypesSqlConnector.create(sqlService, from);

        String to = createRandomTopic();
        kafkaMapping(to, ID_SCHEMA, ALL_TYPES_SCHEMA)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "string VARCHAR",
                        "\"boolean\" BOOLEAN",
                        "byte TINYINT",
                        "short SMALLINT",
                        "\"int\" INT",
                        "long BIGINT",
                        "\"float\" REAL",
                        "\"double\" DOUBLE",
                        "\"decimal\" DECIMAL",
                        "\"time\" TIME",
                        "\"date\" DATE",
                        "\"timestamp\" TIMESTAMP",
                        "timestampTz TIMESTAMP WITH TIME ZONE",
                        "map OBJECT",
                        "object OBJECT")
                .create();

        sqlService.execute("INSERT INTO " + to + " SELECT 1, f.* FROM " + from + " f");

        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + to,
                List.of(new Row(
                        1,
                        "string",
                        true,
                        (byte) 127,
                        (short) 32767,
                        2147483647,
                        9223372036854775807L,
                        1234567890.1f,
                        123451234567890.1,
                        new BigDecimal("9223372036854775.123"),
                        LocalTime.of(12, 23, 34),
                        LocalDate.of(2020, 4, 15),
                        LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
                        OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                        "{42=43}", // object values are stored as strings in avro format
                        null
                ))
        );
    }

    @Test
    public void when_explicitTopLevelField_then_fail_key() {
        when_explicitTopLevelField_then_fail("__key", "this");
    }

    @Test
    public void when_explicitTopLevelField_then_fail_this() {
        when_explicitTopLevelField_then_fail("this", "__key");
    }

    private void when_explicitTopLevelField_then_fail(String field, String otherField) {
        assertThatThrownBy(() ->
                kafkaMapping("kafka", NAME_SCHEMA, NAME_SCHEMA)
                        .fields(field + " VARCHAR",
                                "f VARCHAR EXTERNAL NAME \"" + otherField + ".name\"")
                        .create())
                .hasMessage("Cannot use the '" + field + "' field with Avro serialization");
    }

    @Test
    public void test_writingToTopLevel() {
        String name = randomName();
        kafkaMapping(name, ID_SCHEMA, NAME_SCHEMA)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        assertThatThrownBy(() ->
                sqlService.execute("INSERT INTO " + name + "(__key, name) VALUES ('{\"id\":1}', null)"))
                .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");

        assertThatThrownBy(() ->
                sqlService.execute("INSERT INTO " + name + "(id, this) VALUES (1, '{\"name\":\"foo\"}')"))
                .hasMessageContaining("Writing to top-level fields of type OBJECT not supported");
    }

    @Test
    public void test_topLevelFieldExtraction() {
        String name = createRandomTopic();
        kafkaMapping(name, ID_SCHEMA, NAME_SCHEMA)
                .fields("id INT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        insertRecord(1, "Alice");

        assertRowsEventuallyInAnyOrder(
                "SELECT __key, this FROM " + name,
                List.of(new Row(
                        new GenericRecordBuilder(ID_SCHEMA).set("id", 1).build(),
                        new GenericRecordBuilder(NAME_SCHEMA).set("name", "Alice").build()
                ))
        );
    }

    @Test
    public void test_explicitKeyAndValueSerializers() {
        String name = createRandomTopic();
        Class<?> serializerClass = useSchemaRegistry ? KafkaAvroSerializer.class : HazelcastKafkaAvroSerializer.class;
        Class<?> deserializerClass = useSchemaRegistry ? KafkaAvroDeserializer.class : HazelcastKafkaAvroDeserializer.class;

        kafkaMapping(name, NAME_SCHEMA, NAME_SCHEMA)
                .fields("key_name VARCHAR EXTERNAL NAME \"__key.name\"",
                        "value_name VARCHAR EXTERNAL NAME \"this.name\"")
                .options("key.serializer", serializerClass.getCanonicalName(),
                         "key.deserializer", deserializerClass.getCanonicalName(),
                         "value.serializer", serializerClass.getCanonicalName(),
                         "value.deserializer", deserializerClass.getCanonicalName())
                .create();

        insertAndAssertRecord("Alice", "Bob");
    }

    @Test
    public void test_keyAndValueSchemaAvailableInInformationSchema() {
        assumeFalse(useSchemaRegistry);
        String name = createRandomTopic();
        kafkaMapping(name, ID_SCHEMA, NAME_SCHEMA)
                .fields("id BIGINT EXTERNAL NAME \"__key.id\"",
                        "name VARCHAR")
                .create();

        String options = sqlService.execute("SELECT mapping_options FROM information_schema.mappings")
                .iterator().next().getObject(0);
        BiConsumer<String, Schema> assertContainsSchema = (option, schema) -> assertContains(
                options,
                String.format("\"%s\":\"%s\"", option, schema.toString().replace("\"", "\\\""))
        );

        assertContainsSchema.accept(OPTION_KEY_AVRO_SCHEMA, keySchema);
        assertContainsSchema.accept(OPTION_VALUE_AVRO_SCHEMA, valueSchema);
    }

    @Test
    public void test_schemaIdForTwoQueriesIsEqual() {
        assumeTrue(useSchemaRegistry);
        String name = createRandomTopic();
        kafkaMapping(name)
                .fields("__key INT",
                        "field1 VARCHAR")
                .options(OPTION_KEY_FORMAT, JAVA_FORMAT,
                         OPTION_KEY_CLASS, Integer.class.getCanonicalName())
                .create();

        insertRecord(42, "foo");
        insertRecord(43, "bar");

        try (KafkaConsumer<Integer, byte[]> consumer = kafkaTestSupport.createConsumer(
                IntegerDeserializer.class, ByteArrayDeserializer.class, emptyMap(), name)
        ) {
            long timeLimit = System.nanoTime() + SECONDS.toNanos(10);
            List<Integer> schemaIds = new ArrayList<>();
            while (schemaIds.size() < 2) {
                if (System.nanoTime() > timeLimit) {
                    Assert.fail("Timeout waiting for the records from Kafka");
                }
                ConsumerRecords<Integer, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Integer, byte[]> record : records) {
                    // First byte is MAGIC_BYTE: https://github.com/confluentinc/schema-registry/blob/760dfbcfc6aa2269271e1f5f64870b2adf3cafa2/avro-serializer/src/main/java/io/confluent/kafka/serializers/AbstractKafkaAvroSerializer.java#L135
                    int id = Bits.readInt(record.value(), 1, true);
                    schemaIds.add(id);
                }
            }
            assertEquals("The schemaIds of the two records don't match", schemaIds.get(0), schemaIds.get(1));
        }
    }

    private static String createRandomTopic() {
        return createRandomTopic(INITIAL_PARTITION_COUNT);
    }

    private void insertRecord(Object... values) {
        insertLiterals(instance(), mapping.name, values);
    }

    private void insertAndAssertRecord(Object... values) {
        insertAndAssertRecord(values, values, values);
    }

    private void insertAndAssertRecord(@Nonnull Object[] sqlValues, @Nonnull Object[] avroValues) {
        insertAndAssertRecord(sqlValues, avroValues, sqlValues);
    }

    private void insertAndAssertRecord(@Nonnull Object[] insertValues, @Nonnull Object[] avroValues,
                                       @Nonnull Object[] selectValues) {
        insertRecord(insertValues);

        kafkaTestSupport.assertTopicContentsEventually(
                mapping.name,
                Map.of(
                        createRecord(keySchema, copyOfRange(mapping.fields, 0, 1),
                                copyOfRange(avroValues, 0, 1)),
                        createRecord(valueSchema, copyOfRange(mapping.fields, 1, mapping.fields.length),
                                copyOfRange(avroValues, 1, mapping.fields.length))
                ),
                useSchemaRegistry ? KafkaAvroDeserializer.class : HazelcastKafkaAvroDeserializer.class,
                useSchemaRegistry ? KafkaAvroDeserializer.class : HazelcastKafkaAvroDeserializer.class,
                clientProperties
        );
        assertRowsEventuallyInAnyOrder(
                "SELECT * FROM " + mapping.name,
                List.of(new Row(selectValues))
        );
    }

    @SuppressWarnings("unchecked")
    private static <T1, T2, T3, T4> List<Tuple4<T1, T2, T3, T4>> cartesian(List<Tuple2<T1, T2>> list1,
                                                                           List<Tuple2<T3, T4>> list2) {
        return Lists.cartesianProduct(list1, list2).stream()
                .map(t -> tuple4((T1) t.get(0).f0(), (T2) t.get(0).f1(), (T3) t.get(1).f0(), (T4) t.get(1).f1()))
                .collect(toList());
    }

    /** Generates a type tree on creation. */
    private class KafkaMapping extends SqlMapping {
        KafkaMapping(String name) {
            super(name, KafkaSqlConnector.class);
        }

        @Override
        protected void create(HazelcastInstance instance, boolean replace, boolean ifNotExists) {
            super.create(instance, replace, ifNotExists);
            mapping = toTypeTree();
        }
    }
}
