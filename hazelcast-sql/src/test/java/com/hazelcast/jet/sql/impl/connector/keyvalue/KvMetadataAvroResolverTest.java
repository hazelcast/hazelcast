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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.jet.sql.impl.extract.AvroQueryTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.AvroUpsertTargetDescriptor;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver.INSTANCE;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver.Schemas.OBJECT_SCHEMA;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KvMetadataAvroResolverTest {

    @Parameters(name = "{1}")
    public static Object[] parameters() {
        return new Object[][]{{true, "__key"}, {false, "this"}};
    }

    @Parameter(0) public boolean isKey;
    @Parameter(1) public String prefix;

    @Test
    public void test_resolveFields() {
        Stream<MappingField> fields = INSTANCE.resolveAndValidateFields(
                isKey,
                List.of(field("field", QueryDataType.INT)),
                emptyMap(),
                null
        );

        assertThat(fields).containsExactly(field("field", QueryDataType.INT));
    }

    @Test
    public void when_noKeyOrThisPrefixInExternalName_then_usesValue() {
        KvMetadata metadata = INSTANCE.resolveMetadata(
                isKey,
                List.of(field("field", QueryDataType.INT, "extField")),
                emptyMap(),
                null
        );
        assertThat(metadata.getFields()).containsExactly(isKey
                ? new MapTableField[]{
                        new MapTableField("__key", QueryDataType.OBJECT, true, QueryPath.KEY_PATH)
                }
                : new MapTableField[]{
                        new MapTableField("field", QueryDataType.INT, false, new QueryPath("extField", false)),
                        new MapTableField("this", QueryDataType.OBJECT, true, QueryPath.VALUE_PATH)
                });
    }

    @Test
    public void when_duplicateExternalName_then_throws() {
        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(
                isKey,
                List.of(
                        field("field1", QueryDataType.INT, prefix + ".field"),
                        field("field2", QueryDataType.VARCHAR, prefix + ".field")
                ),
                emptyMap(),
                null
        )).isInstanceOf(QueryException.class)
          .hasMessageMatching("Duplicate external name: (__key|this).field");
    }

    @Test
    public void when_schemaIsNotRecord_then_throws() {
        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(
                isKey,
                List.of(field("field", QueryDataType.INT)),
                Map.of(isKey ? OPTION_KEY_AVRO_SCHEMA : OPTION_VALUE_AVRO_SCHEMA,
                        Schema.create(Schema.Type.INT).toString()),
                null
        )).hasMessage("Schema must be an Avro record");
    }

    @Test
    public void when_inlineSchemaUsedWithSchemaRegistry_then_throws() {
        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(
                isKey,
                List.of(field("field", QueryDataType.INT)),
                Map.of(
                        isKey ? OPTION_KEY_AVRO_SCHEMA : OPTION_VALUE_AVRO_SCHEMA,
                                SchemaBuilder.record("jet.sql").fields()
                                        .optionalInt("field")
                                        .endRecord().toString(),
                        "schema.registry.url", "http://localhost:8081"
                ),
                null
        )).hasMessage("Inline schema cannot be used with schema registry");
    }

    @Test
    public void when_schemaHasUnsupportedType_then_fieldResolutionFails() {
        List<Schema> unsupportedSchemaTypes = List.of(
                Schema.create(Schema.Type.BYTES),
                SchemaBuilder.array().items(Schema.create(Schema.Type.INT)),
                SchemaBuilder.map().values(Schema.create(Schema.Type.INT)),
                SchemaBuilder.enumeration("enum").symbols("symbol"),
                SchemaBuilder.fixed("fixed").size(0)
        );
        for (Schema schema : unsupportedSchemaTypes) {
            assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(
                    isKey,
                    emptyList(),
                    Map.of(isKey ? OPTION_KEY_AVRO_SCHEMA : OPTION_VALUE_AVRO_SCHEMA,
                            SchemaBuilder.record("jet.sql").fields()
                                    .name("field").type(schema).noDefault()
                                    .endRecord().toString()),
                    null
            ).toArray()).hasMessage("Unsupported schema type: " + schema.getType());
        }
    }

    @Test
    public void test_resolveMetadata() {
        KvMetadata metadata = INSTANCE.resolveMetadata(
                isKey,
                List.of(
                        field("string", QueryDataType.VARCHAR),
                        field("boolean", QueryDataType.BOOLEAN),
                        field("byte", QueryDataType.TINYINT),
                        field("short", QueryDataType.SMALLINT),
                        field("int", QueryDataType.INT),
                        field("long", QueryDataType.BIGINT),
                        field("float", QueryDataType.REAL),
                        field("double", QueryDataType.DOUBLE),
                        field("decimal", QueryDataType.DECIMAL),
                        field("time", QueryDataType.TIME),
                        field("date", QueryDataType.DATE),
                        field("timestamp", QueryDataType.TIMESTAMP),
                        field("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME),
                        field("object", QueryDataType.OBJECT)
                ),
                emptyMap(),
                null
        );

        assertThat(metadata.getFields()).containsExactly(
                new MapTableField("string", QueryDataType.VARCHAR, false, QueryPath.create(prefix + ".string")),
                new MapTableField("boolean", QueryDataType.BOOLEAN, false, QueryPath.create(prefix + ".boolean")),
                new MapTableField("byte", QueryDataType.TINYINT, false, QueryPath.create(prefix + ".byte")),
                new MapTableField("short", QueryDataType.SMALLINT, false, QueryPath.create(prefix + ".short")),
                new MapTableField("int", QueryDataType.INT, false, QueryPath.create(prefix + ".int")),
                new MapTableField("long", QueryDataType.BIGINT, false, QueryPath.create(prefix + ".long")),
                new MapTableField("float", QueryDataType.REAL, false, QueryPath.create(prefix + ".float")),
                new MapTableField("double", QueryDataType.DOUBLE, false, QueryPath.create(prefix + ".double")),
                new MapTableField("decimal", QueryDataType.DECIMAL, false, QueryPath.create(prefix + ".decimal")),
                new MapTableField("time", QueryDataType.TIME, false, QueryPath.create(prefix + ".time")),
                new MapTableField("date", QueryDataType.DATE, false, QueryPath.create(prefix + ".date")),
                new MapTableField("timestamp", QueryDataType.TIMESTAMP, false, QueryPath.create(prefix + ".timestamp")),
                new MapTableField("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, false,
                        QueryPath.create(prefix + ".timestampTz")),
                new MapTableField("object", QueryDataType.OBJECT, false, QueryPath.create(prefix + ".object")),
                new MapTableField(prefix, QueryDataType.OBJECT, true, QueryPath.create(prefix))
        );
        assertThat(metadata.getQueryTargetDescriptor()).isEqualTo(AvroQueryTargetDescriptor.INSTANCE);
        assertThat(metadata.getUpsertTargetDescriptor()).isEqualToComparingFieldByField(
                new AvroUpsertTargetDescriptor(SchemaBuilder.record("jet.sql").fields()
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
                        .name("object").type(OBJECT_SCHEMA).withDefault(null)
                        .endRecord()));
    }

    private MappingField field(String name, QueryDataType type) {
        return new MappingField(name, type, prefix + "." + name);
    }

    private static MappingField field(String name, QueryDataType type, String externalName) {
        return new MappingField(name, type, externalName);
    }
}
