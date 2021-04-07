/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class MappingCatalogTest {

    private MappingCatalog catalog;

    @Mock
    private NodeEngine nodeEngine;

    @Mock
    private MappingStorage storage;

    @Mock
    private SqlConnectorCache connectorCache;

    @Mock
    private SqlConnector connector;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);

        catalog = new MappingCatalog(nodeEngine, storage, connectorCache);
    }

    @Test
    public void when_createsInvalidMapping_then_throws() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.type())).willReturn(connector);
        given(connector.resolveAndValidateFields(nodeEngine, mapping.options(), mapping.fields()))
                .willThrow(new RuntimeException("expected test exception"));

        // when
        // then
        assertThatThrownBy(() -> catalog.createMapping(mapping, true, true))
                .hasMessageContaining("expected test exception");
        verifyNoInteractions(storage);
    }

    @Test
    public void when_createsDuplicateMapping_then_throws() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.type())).willReturn(connector);
        given(connector.resolveAndValidateFields(nodeEngine, mapping.options(), mapping.fields()))
                .willReturn(singletonList(new MappingField("field_name", QueryDataType.INT)));
        given(storage.putIfAbsent(mapping.name(), mapping)).willReturn(false);

        // when
        // then
        assertThatThrownBy(() -> catalog.createMapping(mapping, false, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Mapping already exists: name");
    }

    @Test
    public void when_createsDuplicateMappingWithIfNotExists_then_succeeds() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.type())).willReturn(connector);
        given(connector.resolveAndValidateFields(nodeEngine, mapping.options(), mapping.fields()))
                .willReturn(singletonList(new MappingField("field_name", QueryDataType.INT)));
        given(storage.putIfAbsent(mapping.name(), mapping)).willReturn(false);

        // when
        catalog.createMapping(mapping, false, true);

        // then
        verify(storage).putIfAbsent(eq(mapping.name()), isA(Mapping.class));
    }

    @Test
    public void when_replacesMapping_then_succeeds() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.type())).willReturn(connector);
        given(connector.resolveAndValidateFields(nodeEngine, mapping.options(), mapping.fields()))
                .willReturn(singletonList(new MappingField("field_name", QueryDataType.INT)));

        // when
        catalog.createMapping(mapping, true, false);

        // then
        verify(storage).put(eq(mapping.name()), isA(Mapping.class));
    }

    @Test
    public void when_removesNonExistingMapping_then_throws() {
        // given
        String name = "name";

        given(storage.remove(name)).willReturn(false);

        // when
        // then
        assertThatThrownBy(() -> catalog.removeMapping(name, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Mapping does not exist: name");
    }

    @Test
    public void when_removesNonExistingMappingWithIfExists_then_succeeds() {
        // given
        String name = "name";

        given(storage.remove(name)).willReturn(false);

        // when
        // then
        catalog.removeMapping(name, true);
    }

    private static Mapping mapping() {
        return new Mapping("name", "external_name", "type", emptyList(), emptyMap());
    }
}
