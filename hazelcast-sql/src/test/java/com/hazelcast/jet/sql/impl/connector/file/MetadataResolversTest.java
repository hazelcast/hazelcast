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

package com.hazelcast.jet.sql.impl.connector.file;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_GLOB;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_PATH;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

@RunWith(MockitoJUnitRunner.class)
public class MetadataResolversTest {

    private static final String FORMAT = "some-format";

    private MetadataResolvers resolvers;

    @Mock
    private MetadataResolver<?> resolver;

    @Mock
    private Metadata metadata;

    @Before
    public void setUp() {
        given(resolver.supportedFormat()).willReturn(FORMAT);

        resolvers = new MetadataResolvers(resolver);
    }

    @Test
    public void when_formatIsMissing_then_throws() {
        assertThatThrownBy(() -> resolvers.resolveAndValidateFields(emptyList(), emptyMap()))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Missing 'format");
    }

    @Test
    public void when_pathIsMissing_then_throws() {
        assertThatThrownBy(() -> resolvers.resolveAndValidateFields(emptyList(), singletonMap(OPTION_FORMAT, FORMAT)))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Missing 'path");
    }

    @Test
    public void when_formatIsNotSupported_then_throws() {
        assertThatThrownBy(() -> resolvers.resolveAndValidateFields(
                emptyList(),
                ImmutableMap.of(OPTION_FORMAT, "some-other-format", OPTION_PATH, "/path")
        )).isInstanceOf(QueryException.class)
          .hasMessageContaining("Unsupported serialization format");
    }

    @Test
    public void test_resolveAndValidateFields() {
        // given
        Map<String, String> options = ImmutableMap.of(OPTION_FORMAT, FORMAT, OPTION_PATH, "/path", OPTION_GLOB, "*");

        given(resolver.resolveAndValidateFields(emptyList(), options))
                .willReturn(singletonList(new MappingField("field", QueryDataType.VARCHAR)));

        // when
        List<MappingField> resolvedFields = resolvers.resolveAndValidateFields(emptyList(), options);

        // then
        assertThat(resolvedFields).containsOnly(new MappingField("field", QueryDataType.VARCHAR));
    }

    @Test
    public void test_resolveMetadata() {
        // given
        List<MappingField> resolvedFields = singletonList(new MappingField("field", QueryDataType.VARCHAR));
        Map<String, String> options = ImmutableMap.of(OPTION_FORMAT, FORMAT, OPTION_PATH, "/path", OPTION_GLOB, "*");

        given(resolver.resolveMetadata(resolvedFields, options)).willReturn(metadata);

        // when
        Metadata metadata = resolvers.resolveMetadata(resolvedFields, options);

        // then
        assertThat(metadata).isNotNull();
    }
}
