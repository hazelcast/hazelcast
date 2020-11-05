/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.inject.PortableUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

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
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(1, 2, 3)
                        .addUTFField("string")
                        .addCharField("character")
                        .addBooleanField("boolean")
                        .addByteField("byte")
                        .addShortField("short")
                        .addIntField("int")
                        .addLongField("long")
                        .addFloatField("float")
                        .addDoubleField("double")
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), String.valueOf(classDefinition.getFactoryId()),
                (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), String.valueOf(classDefinition.getClassId()),
                (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), String.valueOf(classDefinition.getVersion())
        );

        List<MappingField> fields = INSTANCE.resolveAndValidateFields(key, emptyList(), options, ss);

        assertThat(fields).containsExactly(
                field("string", QueryDataType.VARCHAR, prefix + ".string"),
                field("character", QueryDataType.VARCHAR_CHARACTER, prefix + ".character"),
                field("boolean", QueryDataType.BOOLEAN, prefix + ".boolean"),
                field("byte", QueryDataType.TINYINT, prefix + ".byte"),
                field("short", QueryDataType.SMALLINT, prefix + ".short"),
                field("int", QueryDataType.INT, prefix + ".int"),
                field("long", QueryDataType.BIGINT, prefix + ".long"),
                field("float", QueryDataType.REAL, prefix + ".float"),
                field("double", QueryDataType.DOUBLE, prefix + ".double")
        );
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_userDeclaresField_then_itsNameHasPrecedenceOverResolvedOne(boolean key, String prefix) {
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

        List<MappingField> fields = INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("renamed_field", QueryDataType.INT, prefix + ".field")),
                options,
                ss
        );

        assertThat(fields).containsExactly(
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
                        .addUTFField("field2")
                        .build();
        ss.getPortableContext().registerClassDefinition(classDefinition);
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID), String.valueOf(classDefinition.getFactoryId()),
                (key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID), String.valueOf(classDefinition.getClassId()),
                (key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION), String.valueOf(classDefinition.getVersion())
        );

        List<MappingField> fields = INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("field2", QueryDataType.VARCHAR, prefix + ".field2")),
                options,
                ss
        );

        assertThat(fields).containsExactly(
                field("field2", QueryDataType.VARCHAR, prefix + ".field2")
        );
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_typeMismatchBetweenDeclaredAndSchemaField_then_throws(boolean key, String prefix) {
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
                .isEqualToComparingFieldByField(new PortableUpsertTargetDescriptor(
                        classDefinition.getFactoryId(),
                        classDefinition.getClassId(),
                        classDefinition.getVersion())
                );
    }

    private static MappingField field(String name, QueryDataType type, String externalName) {
        return new MappingField(name, type, externalName);
    }
}
