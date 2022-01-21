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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.sql.impl.inject.PojoUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver.INSTANCE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class KvMetadataJavaResolverTest {

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolvePrimitiveField(boolean key, String path) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), int.class.getName()
        );

        Stream<MappingField> fields = INSTANCE.resolveAndValidateFields(key, emptyList(), options, null);

        assertThat(fields).containsExactly(field(path, QueryDataType.INT, QueryPath.create(path).toString()));
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_renamesPrimitiveField_then_throws(boolean key, String path) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), int.class.getName()
        );

        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("renamed_field", QueryDataType.INT, path)),
                options,
                null
        )).isInstanceOf(QueryException.class)
                .hasMessageMatching("Cannot rename field: '" + path + '\'');
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_typeMismatchBetweenPrimitiveDeclaredAndSchemaField_then_throws(boolean key, String path) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), int.class.getName()
        );

        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field(path, QueryDataType.VARCHAR, path)),
                options,
                null
        )).isInstanceOf(QueryException.class)
                .hasMessageMatching("Mismatch between declared and resolved type for field '(__key|this)'");
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_userDeclaresPrimitiveAdditionalField_then_throws(boolean key, String prefix) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), int.class.getName()
        );

        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("field", QueryDataType.INT, prefix + ".field")),
                options,
                null
        )).isInstanceOf(QueryException.class)
                .hasMessage("The field '" + prefix + "' is of type INTEGER, you can't map '" + prefix + ".field' too");
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolvePrimitiveMetadata(boolean key, String path) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), int.class.getName()
        );

        KvMetadata metadata = INSTANCE.resolveMetadata(
                key,
                singletonList(field(path, QueryDataType.INT, path)),
                options,
                null
        );

        assertThat(metadata.getFields()).containsExactly(
                new MapTableField(path, QueryDataType.INT, false, QueryPath.create(path))
        );
        assertThat(metadata.getQueryTargetDescriptor()).isEqualTo(GenericQueryTargetDescriptor.DEFAULT);
        assertThat(metadata.getUpsertTargetDescriptor()).isEqualTo(PrimitiveUpsertTargetDescriptor.INSTANCE);
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolveObjectFields(boolean key, String prefix) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), Type.class.getName()
        );

        Stream<MappingField> fields = INSTANCE.resolveAndValidateFields(key, emptyList(), options, null);

        assertThat(fields).containsExactly(field("field", QueryDataType.INT, prefix + ".field"));
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_userDeclaresObjectField_then_itsNameHasPrecedenceOverResolvedOne(boolean key, String prefix) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), Type.class.getName()
        );

        Stream<MappingField> fields = INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("renamed_field", QueryDataType.INT, prefix + ".field")),
                options,
                null
        );

        assertThat(fields).containsExactly(field("renamed_field", QueryDataType.INT, prefix + ".field"));
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_userDeclaresFields_then_fieldsFromClassNotAdded(boolean key, String prefix) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), Type.class.getName()
        );

        Stream<MappingField> fields = INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("field2", QueryDataType.VARCHAR, prefix + ".field2")),
                options,
                null
        );

        assertThat(fields).containsExactly(field("field2", QueryDataType.VARCHAR, prefix + ".field2"));
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_typeMismatchBetweenObjectDeclaredAndSchemaField_then_throws(boolean key, String prefix) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), Type.class.getName()
        );

        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(
                key,
                singletonList(field("field", QueryDataType.VARCHAR, prefix + ".field")),
                options,
                null
        )).isInstanceOf(QueryException.class)
                .hasMessageContaining("Mismatch between declared and resolved type for field 'field'");
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void when_noKeyOrThisPrefixInExternalName_then_usesValue(boolean key) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), Object.class.getName()
        );

        KvMetadata metadata = INSTANCE.resolveMetadata(
                key,
                singletonList(field("field", QueryDataType.INT, "extField")),
                options,
                null
        );
        assertThat(metadata.getFields()).containsExactly(
                key
                        ? new MapTableField[]{
                        new MapTableField("__key", QueryDataType.OBJECT, true, QueryPath.KEY_PATH)
                }
                        : new MapTableField[]{
                        new MapTableField("field", QueryDataType.INT, false, new QueryPath("extField", false)),
                        new MapTableField("this", QueryDataType.OBJECT, true, QueryPath.VALUE_PATH)
                });
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void when_userDeclaresObjectDuplicateExternalName_then_throws(boolean key, String prefix) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), Type.class.getName()
        );

        assertThatThrownBy(() -> INSTANCE.resolveAndValidateFields(
                key,
                asList(
                        field("field1", QueryDataType.INT, prefix + ".field"),
                        field("field2", QueryDataType.VARCHAR, prefix + ".field")
                ),
                options,
                null
        )).isInstanceOf(QueryException.class)
                .hasMessageMatching("Duplicate external name: (__key|this).field");
    }

    @Test
    @Parameters({
            "true, __key",
            "false, this"
    })
    public void test_resolveMetadata(boolean key, String prefix) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT,
                (key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS), Type.class.getName()
        );

        KvMetadata metadata = INSTANCE.resolveMetadata(
                key,
                singletonList(field("field", QueryDataType.INT, prefix + ".field")),
                options,
                null
        );

        assertThat(metadata.getFields()).containsExactly(
                new MapTableField("field", QueryDataType.INT, false, QueryPath.create(prefix + ".field")),
                new MapTableField(prefix, QueryDataType.OBJECT, true, QueryPath.create(prefix))
        );
        assertThat(metadata.getQueryTargetDescriptor()).isEqualTo(GenericQueryTargetDescriptor.DEFAULT);
        assertThat(metadata.getUpsertTargetDescriptor())
                .isEqualToComparingFieldByField(new PojoUpsertTargetDescriptor(
                        Type.class.getName(),
                        ImmutableMap.of("field", int.class.getName())
                ));
    }

    private static MappingField field(String name, QueryDataType type, String externalName) {
        return new MappingField(name, type, externalName);
    }

    @SuppressWarnings("unused")
    private static final class Type {

        public int field;
    }
}
