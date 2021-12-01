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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableResolver.TableListener;
import com.hazelcast.sql.impl.schema.view.View;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TableResolverImplTest {

    private TableResolverImpl catalog;

    @Mock
    private NodeEngine nodeEngine;

    @Mock
    private TablesStorage tableStorage;

    @Mock
    private SqlConnectorCache connectorCache;

    @Mock
    private SqlConnector connector;

    @Mock
    private TableListener listener;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);

        catalog = new TableResolverImpl(nodeEngine, tableStorage, connectorCache);
        catalog.registerListener(listener);
    }

    // region mapping storage tests

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
        verify(tableStorage, never()).putIfAbsent(anyString(), (Mapping) any());
        verify(tableStorage, never()).put(anyString(), (Mapping) any());
        verifyNoInteractions(listener);
    }

    @Test
    public void when_createsDuplicateMapping_then_throws() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.type())).willReturn(connector);
        given(connector.resolveAndValidateFields(nodeEngine, mapping.options(), mapping.fields()))
                .willReturn(singletonList(new MappingField("field_name", INT)));
        given(tableStorage.putIfAbsent(eq(mapping.name()), isA(Mapping.class))).willReturn(false);

        // when
        // then
        assertThatThrownBy(() -> catalog.createMapping(mapping, false, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Mapping or view already exists: name");
        verifyNoInteractions(listener);
    }

    @Test
    public void when_createsDuplicateMappingWithIfNotExists_then_succeeds() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.type())).willReturn(connector);
        given(connector.resolveAndValidateFields(nodeEngine, mapping.options(), mapping.fields()))
                .willReturn(singletonList(new MappingField("field_name", INT)));
        given(tableStorage.putIfAbsent(eq(mapping.name()), isA(Mapping.class))).willReturn(false);

        // when
        catalog.createMapping(mapping, false, true);

        // then
        verifyNoInteractions(listener);
    }

    @Test
    public void when_replacesMapping_then_succeeds() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.type())).willReturn(connector);
        given(connector.resolveAndValidateFields(nodeEngine, mapping.options(), mapping.fields()))
                .willReturn(singletonList(new MappingField("field_name", INT)));

        // when
        catalog.createMapping(mapping, true, false);

        // then
        verify(tableStorage).put(eq(mapping.name()), isA(Mapping.class));
        verify(listener).onTableChanged();
    }

    @Test
    public void when_removesExistingMapping_then_callsListeners() {
        // given
        String name = "name";

        given(tableStorage.removeMapping(name)).willReturn(mapping());

        // when
        // then
        catalog.removeMapping(name, false);
        verify(listener).onTableChanged();
    }

    @Test
    public void when_removesNonExistingMapping_then_throws() {
        // given
        String name = "name";

        given(tableStorage.removeMapping(name)).willReturn(null);

        // when
        // then
        assertThatThrownBy(() -> catalog.removeMapping(name, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Mapping does not exist: name");
        verifyNoInteractions(listener);
    }

    @Test
    public void when_removesNonExistingMappingWithIfExists_then_succeeds() {
        // given
        String name = "name";

        given(tableStorage.removeMapping(name)).willReturn(null);

        // when
        // then
        catalog.removeMapping(name, true);
        verifyNoInteractions(listener);
    }

    // endregion

    // region view storage tests

    @Test
    public void when_createsView_then_succeeds() {
        // given
        View view = view();
        given(tableStorage.putIfAbsent(view.name(), view)).willReturn(true);

        // when
        catalog.createView(view, false, false);

        // then
        verify(tableStorage).putIfAbsent(eq(view.name()), isA(View.class));
    }

    @Test
    public void when_createsViewIfNotExists_then_succeeds() {
        // given
        View view = view();
        given(tableStorage.putIfAbsent(view.name(), view)).willReturn(true);

        // when
        catalog.createView(view, false, true);

        // then
        verify(tableStorage).putIfAbsent(eq(view.name()), isA(View.class));
    }

    @Test
    public void when_createsDuplicateViewsIfReplace_then_succeeds() {
        // given
        View view = view();

        // when
        catalog.createView(view, true, false);

        // then
        verify(tableStorage).put(eq(view.name()), isA(View.class));
    }

    @Test
    public void when_createsDuplicateViewsIfReplaceAndIfNotExists_then_succeeds() {
        // given
        View view = view();

        // when
        catalog.createView(view, true, true);

        // then
        verify(tableStorage).putIfAbsent(eq(view.name()), isA(View.class));
    }

    @Test
    public void when_createsDuplicateViews_then_throws() {
        // given
        View view = view();
        given(tableStorage.putIfAbsent(eq(view.name()), isA(View.class))).willReturn(false);

        // when
        // then
        assertThatThrownBy(() -> catalog.createView(view, false, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Mapping or view already exists: name");
        verifyNoInteractions(listener);
    }

    @Test
    public void when_removesNonExistingView_then_throws() {
        // given
        String name = "name";

        given(tableStorage.removeView(name)).willReturn(null);

        // when
        // then
        assertThatThrownBy(() -> catalog.removeView(name, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("View does not exist: name");
        verifyNoInteractions(listener);
    }

    @Test
    public void when_removesNonExistingViewWithIfExists_then_succeeds() {
        // given
        String name = "name";

        given(tableStorage.removeView(name)).willReturn(null);

        // when
        // then
        catalog.removeView(name, true);
        verifyNoInteractions(listener);
    }

    // endregion

    private static Mapping mapping() {
        return new Mapping("name", "external_name", "type", emptyList(), emptyMap());
    }

    private static View view() {
        return new View("name", "SELECT * FROM map", singletonList("*"), asList(OBJECT, OBJECT));
    }
}
