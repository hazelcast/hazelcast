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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@RunWith(JUnitParamsRunner.class)
public class KvMetadataResolversTest {

    private KvMetadataResolvers resolvers;

    @Mock
    private KvMetadataResolver resolver;

    @Mock
    private NodeEngine nodeEngine;

    @Mock
    private InternalSerializationService ss;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        given(nodeEngine.getSerializationService()).willReturn(ss);
        given(resolver.supportedFormats())
                .willAnswer((Answer<Stream<String>>) invocationOnMock -> Stream.of(JAVA_FORMAT));

        resolvers = new KvMetadataResolvers(resolver);
    }

    @Test
    @Parameters({
            "__key",
            "this"
    })
    public void when_renamedKeyOrThis_then_throws(String fieldName) {
        MappingField field = field(fieldName, QueryDataType.INT, "renamed");
        assertThatThrownBy(() -> resolvers.resolveAndValidateFields(singletonList(field), emptyMap(), nodeEngine))
                .hasMessage("Cannot rename field: '" + fieldName + '\'');
    }

    @Test
    public void when_invalidExternalName_then_throws() {
        MappingField field = field("field_name", QueryDataType.INT, "invalid_prefix.name");
        assertThatThrownBy(() -> resolvers.resolveAndValidateFields(singletonList(field), emptyMap(), nodeEngine))
                .hasMessage("Invalid external name: " + "invalid_prefix.name");
    }

    @Test
    public void test_resolveAndValidateFields() {
        Map<String, String> options = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT
        );
        given(resolver.resolveAndValidateFields(true, emptyList(), options, ss))
                .willReturn(Stream.of(field("__key", QueryDataType.INT)));
        given(resolver.resolveAndValidateFields(false, emptyList(), options, ss))
                .willReturn(Stream.of(field("this", QueryDataType.VARCHAR)));

        List<MappingField> fields = resolvers.resolveAndValidateFields(emptyList(), options, nodeEngine);

        assertThat(fields).containsExactly(
                field("__key", QueryDataType.INT),
                field("this", QueryDataType.VARCHAR)
        );
    }

    @Test
    public void when_keyOrThisNameIsUsed_then_itIsFilteredOut() {
        Map<String, String> options = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT
        );
        given(resolver.resolveAndValidateFields(true, emptyList(), options, ss))
                .willReturn(Stream.of(
                        field("__key", QueryDataType.INT, "__key.name"),
                        field("keyField", QueryDataType.INT, "__key.__keyField")
                ));
        given(resolver.resolveAndValidateFields(false, emptyList(), options, ss))
                .willReturn(Stream.of(
                        field("this", QueryDataType.VARCHAR, "this.name"),
                        field("thisField", QueryDataType.VARCHAR, "this.thisField")
                ));

        List<MappingField> fields = resolvers.resolveAndValidateFields(emptyList(), options, nodeEngine);

        assertThat(fields).containsExactly(
                field("keyField", QueryDataType.INT, "__key.__keyField"),
                field("thisField", QueryDataType.VARCHAR, "this.thisField")
        );
    }

    @Test
    public void when_keyClashesWithValue_then_keyIsChosen() {
        Map<String, String> options = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT
        );
        given(resolver.resolveAndValidateFields(true, emptyList(), options, ss))
                .willReturn(Stream.of(field("field", QueryDataType.INT, "__key.field")));
        given(resolver.resolveAndValidateFields(false, emptyList(), options, ss))
                .willReturn(Stream.of(field("field", QueryDataType.VARCHAR, "this.field")));

        List<MappingField> fields = resolvers.resolveAndValidateFields(emptyList(), options, nodeEngine);

        assertThat(fields).containsExactly(field("field", QueryDataType.INT, "__key.field"));
    }

    @Test
    public void when_keyFieldsEmpty_then_doesNotFail() {
        Map<String, String> options = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT
        );
        given(resolver.resolveAndValidateFields(true, emptyList(), options, ss))
                .willReturn(Stream.empty());
        given(resolver.resolveAndValidateFields(false, emptyList(), options, ss))
                .willReturn(Stream.of(field("this", QueryDataType.INT)));

        List<MappingField> fields = resolvers.resolveAndValidateFields(emptyList(), options, nodeEngine);
        assertThat(fields).containsExactly(field("this", QueryDataType.INT));
    }

    @Test
    public void when_valueFieldsEmpty_then_doesNotFail() {
        Map<String, String> options = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT
        );
        given(resolver.resolveAndValidateFields(true, emptyList(), options, ss))
                .willReturn(Stream.of(field("__key", QueryDataType.INT)));
        given(resolver.resolveAndValidateFields(false, emptyList(), options, ss))
                .willReturn(Stream.empty());

        List<MappingField> fields = resolvers.resolveAndValidateFields(emptyList(), options, nodeEngine);
        assertThat(fields).containsExactly(field("__key", QueryDataType.INT));
    }

    @Test
    public void when_keyAndValueFieldsEmpty_then_throws() {
        Map<String, String> options = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT
        );
        given(resolver.resolveAndValidateFields(true, emptyList(), options, ss))
                .willReturn(Stream.empty());
        given(resolver.resolveAndValidateFields(false, emptyList(), options, ss))
                .willReturn(Stream.empty());

        assertThatThrownBy(() -> resolvers.resolveAndValidateFields(emptyList(), options, nodeEngine))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("The resolved field list is empty");
    }

    @Test
    public void when_formatIsMissingInOptionsWhileResolvingFields_then_throws() {
        assertThatThrownBy(() -> resolvers.resolveAndValidateFields(emptyList(), emptyMap(), nodeEngine))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Missing 'keyFormat' option");
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_resolveMetadata(boolean key) {
        Map<String, String> options = ImmutableMap.of(
                (key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT), JAVA_FORMAT
        );
        given(resolver.resolveMetadata(key, emptyList(), options, ss)).willReturn(mock(KvMetadata.class));

        KvMetadata metadata = resolvers.resolveMetadata(key, emptyList(), options, ss);

        assertThat(metadata).isNotNull();
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void when_formatIsMissingInOptionsWhileResolvingMetadata_then_throws(boolean key) {
        assertThatThrownBy(() -> resolvers.resolveMetadata(key, emptyList(), emptyMap(), ss))
                .isInstanceOf(QueryException.class)
                .hasMessageMatching("Missing '(key|value)Format' option");
    }

    private static MappingField field(String name, QueryDataType type) {
        return field(name, type, name);
    }

    private static MappingField field(String name, QueryDataType type, String externalName) {
        return new MappingField(name, type, externalName);
    }
}
