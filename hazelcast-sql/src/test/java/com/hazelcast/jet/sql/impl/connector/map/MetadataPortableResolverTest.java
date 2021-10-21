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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.inject.PortableUpsertTargetDescriptor;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.map.MetadataPortableResolver.INSTANCE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class MetadataPortableResolverTest {

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolveFields(boolean key, String prefix) {
        Stream<MappingField> resolvedFields = INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("field", QueryDataType.INT, prefix + ".field")),
                ImmutableMap.of(
                        (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), "1",
                        (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), "2",
                        (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), "3"
                ),
                new DefaultSerializationServiceBuilder().build()
        );

        assertThat(resolvedFields).containsExactly(field("field", QueryDataType.INT, prefix + ".field"));
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolveFieldsFromClassDefinition(boolean key, String prefix) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addStringField("string")
                        .addCharField("character")
                        .addBooleanField("boolean")
                        .addByteField("byte")
                        .addShortField("short")
                        .addIntField("int")
                        .addLongField("long")
                        .addFloatField("float")
                        .addDoubleField("double")
                        .addDecimalField("decimal")
                        .addTimeField("time")
                        .addDateField("date")
                        .addTimestampField("timestamp")
                        .addTimestampWithTimezoneField("timestampTz")
                        .addPortableField("object", new ClassDefinitionBuilder(4, 5, 6).build())
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), String.valueOf(classDefinition.getFactoryId()),
                (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), String.valueOf(classDefinition.getClassId()),
                (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), String.valueOf(classDefinition.getVersion())
        );

        Stream<MappingField> resolvedFields = INSTANCE.resolveAndValidateFields(key, emptyList(), options, ss);

        assertThat(resolvedFields).containsExactly(
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
                field("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, prefix + ".timestampTz"),
                field("object", QueryDataType.OBJECT, prefix + ".object")
        );
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolveFieldsWithoutClassDefinition(boolean key, String prefix) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), "1",
                (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), "2",
                (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), "3"
        );

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

        Stream<MappingField> resolvedFields = INSTANCE.resolveAndValidateFields(key, fields, options, ss);

        assertThat(resolvedFields).containsExactlyElementsOf(fields);
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_fieldIsObjectAndClassDefinitionDoesNotExist_then_throws(boolean key, String prefix) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), "1",
                (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), "2",
                (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), "3"
        );

        List<MappingField> fields = singletonList(
                field("object", QueryDataType.OBJECT, prefix + ".object")
        );

        //noinspection ResultOfMethodCallIgnored
        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(key, fields, options, ss).collect(toList()))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Cannot derive Portable type for '" + QueryDataTypeFamily.OBJECT + "'");
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_userDeclaresField_then_itsNameHasPrecedenceOverClassDefinitionOne(boolean key, String prefix) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addIntField("field")
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), String.valueOf(classDefinition.getFactoryId()),
                (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), String.valueOf(classDefinition.getClassId()),
                (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), String.valueOf(classDefinition.getVersion())
        );

        Stream<MappingField> resolvedFields = INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("renamed_field", QueryDataType.INT, prefix + ".field")),
                options,
                ss
        );

        assertThat(resolvedFields).containsExactly(
                field("renamed_field", QueryDataType.INT, prefix + ".field")
        );
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_userDeclaresFields_then_fieldsNotAddedFromClassDefinition(boolean key, String prefix) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addIntField("field1")
                        .addStringField("field2")
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), String.valueOf(classDefinition.getFactoryId()),
                (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), String.valueOf(classDefinition.getClassId()),
                (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), String.valueOf(classDefinition.getVersion())
        );

        Stream<MappingField> resolvedFields = INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("field2", QueryDataType.VARCHAR, prefix + ".field2")),
                options,
                ss
        );

        assertThat(resolvedFields).containsExactly(
                field("field2", QueryDataType.VARCHAR, prefix + ".field2")
        );
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_typeMismatchBetweenDeclaredAndClassDefinitionField_then_throws(boolean key, String prefix) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addIntField("field")
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), String.valueOf(classDefinition.getFactoryId()),
                (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), String.valueOf(classDefinition.getClassId()),
                (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), String.valueOf(classDefinition.getVersion())
        );

        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("field", QueryDataType.VARCHAR, prefix + ".field")),
                options,
                ss
        )).isInstanceOf(QueryException.class)
          .hasMessageContaining("Mismatch between declared and resolved type: field");
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_objectUsedForCurrentlyUnknownType_then_allowed(boolean key, String prefix) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ClassDefinition nestedClassDef =
                new ClassDefinitionBuilder(1, 3, 4)
                        .addIntField("intField")
                        .build();
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addIntArrayField("intArrayField")
                        .addPortableField("nestedPortableField", nestedClassDef)
                        .build();
        ss.getPortableContext().registerClassDefinition(nestedClassDef);
        ss.getPortableContext().registerClassDefinition(classDefinition);
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), String.valueOf(classDefinition.getFactoryId()),
                (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), String.valueOf(classDefinition.getClassId()),
                (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), String.valueOf(classDefinition.getVersion())
        );

        // We normally don't allow mapping e.g. INT field as OBJECT. But we need to allow it for arrays and nested objects
        // due to backwards-compatibility.
        INSTANCE.resolveAndValidateFields(
                key,
                asList(
                        field("intArrayField", QueryDataType.OBJECT, prefix + ".intArrayField"),
                        field("nestedPortableField", QueryDataType.OBJECT, prefix + ".nestedPortableField")),
                options,
                ss
        );
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_duplicateExternalName_then_throws(boolean key, String prefix) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addIntField("field")
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), String.valueOf(classDefinition.getFactoryId()),
                (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), String.valueOf(classDefinition.getClassId()),
                (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), String.valueOf(classDefinition.getVersion())
        );

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
                ImmutableMap.of(
                        (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), "1",
                        (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), "2",
                        (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), "3"
                ),
                new DefaultSerializationServiceBuilder().build()
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
        assertThat(metadata.getUpsertTargetDescriptor())
                .isEqualToComparingFieldByField(new PortableUpsertTargetDescriptor(
                        new ClassDefinitionBuilder(1, 2, 3)
                                .addBooleanField("boolean")
                                .addByteField("byte")
                                .addShortField("short")
                                .addIntField("int")
                                .addLongField("long")
                                .addFloatField("float")
                                .addDoubleField("double")
                                .addDecimalField("decimal")
                                .addStringField("string")
                                .addTimeField("time")
                                .addDateField("date")
                                .addTimestampField("timestamp")
                                .addTimestampWithTimezoneField("timestampTz")
                                .build()
                ));
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolveMetadataWithExistingClassDefinition(boolean key, String prefix) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addIntField("field")
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), String.valueOf(classDefinition.getFactoryId()),
                (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), String.valueOf(classDefinition.getClassId()),
                (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), String.valueOf(classDefinition.getVersion())
        );

        KvMetadata metadata = INSTANCE.resolveMetadata(
                key,
                singletonList(field("field", QueryDataType.INT, prefix + ".field")),
                options,
                ss
        );

        assertThat(metadata.getFields()).containsExactly(
                new MapTableField("field", QueryDataType.INT, false, QueryPath.create(prefix + ".field")),
                new MapTableField(prefix, QueryDataType.OBJECT, true, QueryPath.create(prefix))
        );
        assertThat(metadata.getQueryTargetDescriptor()).isEqualTo(GenericQueryTargetDescriptor.DEFAULT);
        assertThat(metadata.getUpsertTargetDescriptor())
                .isEqualToComparingFieldByField(new PortableUpsertTargetDescriptor(classDefinition));
    }

    private static MappingField field(String name, QueryDataType type, String externalName) {
        return new MappingField(name, type, externalName);
    }
}
