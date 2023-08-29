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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.dataconnection.impl.DataConnectionServiceImpl;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.mock.MockUtil;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.openMocks;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataConnectionResolverTest {

    private DataConnectionResolver dataConnectionResolver;

    @Mock
    SqlConnectorCache connectorCache;

    @Mock
    DataConnectionServiceImpl dataConnectionService;

    @Mock
    private DataConnectionStorage relationsStorage;

    private AutoCloseable openMocks;

    @Before
    public void before() {
        openMocks = openMocks(this);
        dataConnectionResolver = new DataConnectionResolver(dataConnectionService, connectorCache, relationsStorage, false);
    }

    @After
    public void cleanUp() {
        MockUtil.closeMocks(openMocks);
    }

    // region dataConnection storage tests

    @Test
    public void when_createDataConnection_then_succeeds() {
        // given
        DataConnectionCatalogEntry dataConnectionCatalogEntry = dataConnection();
        given(relationsStorage.putIfAbsent(dataConnectionCatalogEntry.name(), dataConnectionCatalogEntry)).willReturn(true);

        // when
        dataConnectionResolver.createDataConnection(dataConnectionCatalogEntry, false, false);

        // then
        verify(relationsStorage).putIfAbsent(eq(dataConnectionCatalogEntry.name()), isA(DataConnectionCatalogEntry.class));
    }

    @Test
    public void when_createsDataConnectionIfNotExists_then_succeeds() {
        // given
        DataConnectionCatalogEntry dataConnectionCatalogEntry = dataConnection();
        given(relationsStorage.putIfAbsent(dataConnectionCatalogEntry.name(), dataConnectionCatalogEntry)).willReturn(true);

        // when
        dataConnectionResolver.createDataConnection(dataConnectionCatalogEntry, false, true);

        // then
        verify(relationsStorage).putIfAbsent(eq(dataConnectionCatalogEntry.name()), isA(DataConnectionCatalogEntry.class));
    }

    @Test
    public void when_createsDuplicateDataConnectionIfReplace_then_succeeds() {
        // given
        DataConnectionCatalogEntry dataConnectionCatalogEntry = dataConnection();

        // when
        dataConnectionResolver.createDataConnection(dataConnectionCatalogEntry, true, false);

        // then
        verify(relationsStorage).put(eq(dataConnectionCatalogEntry.name()), isA(DataConnectionCatalogEntry.class));
    }

    @Test
    public void when_createsDuplicateDataConnectionIfReplaceAndIfNotExists_then_succeeds() {
        // given
        DataConnectionCatalogEntry dataConnectionCatalogEntry = dataConnection();

        // when
        dataConnectionResolver.createDataConnection(dataConnectionCatalogEntry, false, true);

        // then
        verify(relationsStorage).putIfAbsent(eq(dataConnectionCatalogEntry.name()), isA(DataConnectionCatalogEntry.class));
    }

    @Test
    public void when_createsDuplicateDataConnection_then_throws() {
        // given
        DataConnectionCatalogEntry dataConnectionCatalogEntry = dataConnection();
        given(relationsStorage.putIfAbsent(eq(dataConnectionCatalogEntry.name()), isA(DataConnectionCatalogEntry.class))).willReturn(false);

        // when
        // then
        assertThatThrownBy(() -> dataConnectionResolver.createDataConnection(dataConnectionCatalogEntry, false, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Data connection already exists: " + dataConnectionCatalogEntry.name());
    }

    @Test
    public void when_removesNonExistingDataConnection_then_throws() {
        // given
        String name = "name";

        given(relationsStorage.removeDataConnection(name)).willReturn(false);

        // when
        // then
        assertThatThrownBy(() -> dataConnectionResolver.removeDataConnection(name, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Data connection does not exist: " + name);
    }

    @Test
    public void when_removesNonExistingDataConnectionWithIfExists_then_succeeds() {
        // given
        String name = "name";

        given(relationsStorage.removeDataConnection(name)).willReturn(false);

        // when
        // then
        dataConnectionResolver.removeDataConnection(name, true);
    }

    // endregion

    private static DataConnectionCatalogEntry dataConnection() {
        return new DataConnectionCatalogEntry();
    }
}
