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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.schema.MappingField;
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

import java.util.List;
import java.util.Map;

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
        MockitoAnnotations.initMocks(this);

        given(nodeEngine.getSerializationService()).willReturn(ss);
        given(resolver.supportedFormat()).willReturn(JAVA_FORMAT);

        resolvers = new KvMetadataResolvers(resolver);
    }

    @Test
    public void test_resolveAndValidateFields() {
        Map<String, String> options = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT
        );
        given(resolver.resolveAndValidateFields(true, emptyList(), options, ss))
                .willReturn(singletonList(field("__key", QueryDataType.INT)));
        given(resolver.resolveAndValidateFields(false, emptyList(), options, ss))
                .willReturn(singletonList(field("this", QueryDataType.VARCHAR)));

        List<MappingField> fields = resolvers.resolveAndValidateFields(emptyList(), options, nodeEngine);

        assertThat(fields).containsExactly(
                field("__key", QueryDataType.INT),
                field("this", QueryDataType.VARCHAR)
        );
    }

    @Test
    public void when_keyClashesWithValue_then_keyIsChosen() {
        Map<String, String> options = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT
        );
        given(resolver.resolveAndValidateFields(true, emptyList(), options, ss))
                .willReturn(singletonList(field("field", QueryDataType.INT)));
        given(resolver.resolveAndValidateFields(false, emptyList(), options, ss))
                .willReturn(singletonList(field("field", QueryDataType.VARCHAR)));

        List<MappingField> fields = resolvers.resolveAndValidateFields(emptyList(), options, nodeEngine);

        assertThat(fields).containsExactly(field("field", QueryDataType.INT));
    }

    @Test
    public void when_keyFieldsEmpty_then_doesNotFail() {
        Map<String, String> options = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT
        );
        given(resolver.resolveAndValidateFields(true, emptyList(), options, ss))
                .willReturn(emptyList());
        given(resolver.resolveAndValidateFields(false, emptyList(), options, ss))
                .willReturn(singletonList(field("this", QueryDataType.INT)));

        List<MappingField> fields = resolvers.resolveAndValidateFields(emptyList(), options, nodeEngine);
        assertThat(fields)
                .isEqualTo(singletonList(new MappingField("this", QueryDataType.INT, null)));
    }

    @Test
    public void when_valueFieldsEmpty_then_doesNotFail() {
        Map<String, String> options = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT
        );
        given(resolver.resolveAndValidateFields(true, emptyList(), options, ss))
                .willReturn(singletonList(field("__key", QueryDataType.INT)));
        given(resolver.resolveAndValidateFields(false, emptyList(), options, ss))
                .willReturn(emptyList());

        List<MappingField> fields = resolvers.resolveAndValidateFields(emptyList(), options, nodeEngine);
        assertThat(fields)
                .isEqualTo(singletonList(new MappingField("__key", QueryDataType.INT, null)));
    }

    @Test
    public void when_keyAndValueFieldsEmpty_then_throws() {
        Map<String, String> options = ImmutableMap.of(
                OPTION_KEY_FORMAT, JAVA_FORMAT,
                OPTION_VALUE_FORMAT, JAVA_FORMAT
        );
        given(resolver.resolveAndValidateFields(true, emptyList(), options, ss))
                .willReturn(emptyList());
        given(resolver.resolveAndValidateFields(false, emptyList(), options, ss))
                .willReturn(emptyList());

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
        return new MappingField(name, type);
    }
}
