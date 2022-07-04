/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.compact.CompactTestUtil;
import com.hazelcast.internal.serialization.impl.compact.FieldDescriptor;
import com.hazelcast.internal.serialization.impl.compact.SchemaWriter;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.inject.CompactUpsertTargetDescriptor;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.map.MetadataCompactResolver.INSTANCE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class MetadataCompactResolverTest {

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_columnListIsRequiredForCompact(boolean key) {
        InternalSerializationService ss = createSerializationService();

        Map<String, String> options =
                ImmutableMap.of((key ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME), "testAll");

        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(key, emptyList(), options, ss))
                .hasMessageContaining("Column list is required for Compact format");
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_objectIsForbiddenForCompact(boolean key, String prefix) {
        InternalSerializationService ss = createSerializationService();

        Map<String, String> options =
                ImmutableMap.of((key ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME), "testAll");

        List<MappingField> fields = asList(field("object", QueryDataType.OBJECT, prefix + ".object"));

        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(key, fields, options, ss).collect(Collectors.toList()))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Cannot derive Compact type for '" + QueryDataTypeFamily.OBJECT + "'");
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolveFields(boolean key, String prefix) {
        InternalSerializationService ss = createSerializationService();
        Map<String, String> options =
                ImmutableMap.of((key ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME), "testAll");

        List<MappingField> fields = asList(
                field("string", QueryDataType.VARCHAR, prefix + ".string"),
                field("character", QueryDataType.VARCHAR_CHARACTER, prefix + ".character"),
                field("boolean", QueryDataType.BOOLEAN, prefix + ".boolean"),
                field("byte", QueryDataType.TINYINT, prefix + ".byte"),
                field("short", QueryDataType.SMALLINT, prefix + ".short"),
                field("int", QueryDataType.INT, prefix + ".int"),
                field("long", QueryDataType.BIGINT, prefix + ".long"),
                field("float", QueryDataType.REAL, prefix + ".float"),
                field("double", QueryDataType.DOUBLE, prefix + ".double"),
                field("decimal", QueryDataType.DECIMAL, prefix + ".decimal"),
                field("time", QueryDataType.TIME, prefix + ".time"),
                field("date", QueryDataType.DATE, prefix + ".date"),
                field("timestamp", QueryDataType.TIMESTAMP, prefix + ".timestamp"),
                field("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, prefix + ".timestampTz")
        );

        Stream<MappingField> resolvedFields = MetadataCompactResolver.INSTANCE.resolveAndValidateFields(key, fields, options, ss);

        assertThat(resolvedFields).containsExactlyElementsOf(fields);
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_typeNameIsRequiredToResolveFields(boolean key, String prefix) {
        InternalSerializationService ss = createSerializationService();

        Map<String, String> options = Collections.emptyMap();

        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(key,
                singletonList(field("field", QueryDataType.INT, prefix + ".field")), options, ss))
                .hasMessageMatching("Unable to resolve table metadata\\. Missing '(key|value)CompactTypeName' option");
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_duplicateExternalName_then_throws(boolean key, String prefix) {
        InternalSerializationService ss = createSerializationService();

        Map<String, String> options =
                ImmutableMap.of((key ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME), "testAll");

        assertThatThrownBy(() -> MetadataPortableResolver.INSTANCE.resolveAndValidateFields(
                key,
                asList(
                        field("field1", QueryDataType.INT, prefix + ".field"),
                        field("field2", QueryDataType.VARCHAR, prefix + ".field")
                ),
                options,
                ss
        )).isInstanceOf(QueryException.class)
                .hasMessageMatching("Duplicate external name: (__key|this).field");
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolveMetadata(boolean key, String prefix) {
        KvMetadata metadata = INSTANCE.resolveMetadata(
                key,
                asList(
                        field("boolean", QueryDataType.BOOLEAN, prefix + ".boolean"),
                        field("byte", QueryDataType.TINYINT, prefix + ".byte"),
                        field("short", QueryDataType.SMALLINT, prefix + ".short"),
                        field("int", QueryDataType.INT, prefix + ".int"),
                        field("long", QueryDataType.BIGINT, prefix + ".long"),
                        field("float", QueryDataType.REAL, prefix + ".float"),
                        field("double", QueryDataType.DOUBLE, prefix + ".double"),
                        field("decimal", QueryDataType.DECIMAL, prefix + ".decimal"),
                        field("string", QueryDataType.VARCHAR, prefix + ".string"),
                        field("time", QueryDataType.TIME, prefix + ".time"),
                        field("date", QueryDataType.DATE, prefix + ".date"),
                        field("timestamp", QueryDataType.TIMESTAMP, prefix + ".timestamp"),
                        field("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, prefix + ".timestampTz")
                ),
                ImmutableMap.of((key ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME), "test"),
                createSerializationService()
        );

        assertThat(metadata.getFields()).containsExactly(
                new MapTableField("boolean", QueryDataType.BOOLEAN, false, QueryPath.create(prefix + ".boolean")),
                new MapTableField("byte", QueryDataType.TINYINT, false, QueryPath.create(prefix + ".byte")),
                new MapTableField("short", QueryDataType.SMALLINT, false, QueryPath.create(prefix + ".short")),
                new MapTableField("int", QueryDataType.INT, false, QueryPath.create(prefix + ".int")),
                new MapTableField("long", QueryDataType.BIGINT, false, QueryPath.create(prefix + ".long")),
                new MapTableField("float", QueryDataType.REAL, false, QueryPath.create(prefix + ".float")),
                new MapTableField("double", QueryDataType.DOUBLE, false, QueryPath.create(prefix + ".double")),
                new MapTableField("decimal", QueryDataType.DECIMAL, false, QueryPath.create(prefix + ".decimal")),
                new MapTableField("string", QueryDataType.VARCHAR, false, QueryPath.create(prefix + ".string")),
                new MapTableField("time", QueryDataType.TIME, false, QueryPath.create(prefix + ".time")),
                new MapTableField("date", QueryDataType.DATE, false, QueryPath.create(prefix + ".date")),
                new MapTableField("timestamp", QueryDataType.TIMESTAMP, false, QueryPath.create(prefix + ".timestamp")),
                new MapTableField(
                        "timestampTz",
                        QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME,
                        false,
                        QueryPath.create(prefix + ".timestampTz")
                ),
                new MapTableField(prefix, QueryDataType.OBJECT, true, QueryPath.create(prefix))
        );
        assertThat(metadata.getQueryTargetDescriptor()).isEqualTo(GenericQueryTargetDescriptor.DEFAULT);
        SchemaWriter schemaWriter = new SchemaWriter("test");
        schemaWriter.addField(new FieldDescriptor("boolean", FieldKind.NULLABLE_BOOLEAN));
        schemaWriter.addField(new FieldDescriptor("byte", FieldKind.NULLABLE_INT8));
        schemaWriter.addField(new FieldDescriptor("short", FieldKind.NULLABLE_INT16));
        schemaWriter.addField(new FieldDescriptor("int", FieldKind.NULLABLE_INT32));
        schemaWriter.addField(new FieldDescriptor("long", FieldKind.NULLABLE_INT64));
        schemaWriter.addField(new FieldDescriptor("float", FieldKind.NULLABLE_FLOAT32));
        schemaWriter.addField(new FieldDescriptor("double", FieldKind.NULLABLE_FLOAT64));
        schemaWriter.addField(new FieldDescriptor("decimal", FieldKind.DECIMAL));
        schemaWriter.addField(new FieldDescriptor("string", FieldKind.STRING));
        schemaWriter.addField(new FieldDescriptor("time", FieldKind.TIME));
        schemaWriter.addField(new FieldDescriptor("date", FieldKind.DATE));
        schemaWriter.addField(new FieldDescriptor("timestamp", FieldKind.TIMESTAMP));
        schemaWriter.addField(new FieldDescriptor("timestampTz", FieldKind.TIMESTAMP_WITH_TIMEZONE));
        assertEquals(metadata.getUpsertTargetDescriptor(), new CompactUpsertTargetDescriptor(schemaWriter.build()));
    }

    private static InternalSerializationService createSerializationService() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.getCompactSerializationConfig().setEnabled(true);
        return new DefaultSerializationServiceBuilder().setSchemaService(CompactTestUtil.createInMemorySchemaService())
                .setConfig(serializationConfig).build();
    }

    private static MappingField field(String name, QueryDataType type, String externalName) {
        return new MappingField(name, type, externalName);
    }
}
