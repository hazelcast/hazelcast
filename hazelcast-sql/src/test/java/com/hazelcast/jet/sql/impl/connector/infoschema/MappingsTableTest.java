/*
 * Copyright 2025 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MappingsTableTest {

    @Test
    public void test_rows() {
        // given
        Mapping mapping = new Mapping(
                "table-name",
                new String[]{"external-schema", "table-external-name"},
                null,
                "table-type",
                null,
                emptyList(),
                singletonMap("key", "value")
        );

        MappingsTable mappingTable = new MappingsTable(
                "catalog",
                null,
                "table-schema",
                singletonList(mapping),
                null,
                (s) -> fail("Should not be invoked"), false);

        // when
        List<Object[]> rows = mappingTable.rows();

        // then
        assertThat(rows).containsExactly(new Object[]{
                "catalog"
                , "table-schema"
                , "table-name"
                , "\"external-schema\".\"table-external-name\""
                , "table-type"
                , "{\"key\":\"value\"}"
        });
    }

    @Test
    public void test_rows_security_enabled() {
        // given
        Mapping mapping = new Mapping(
                "table-name",
                new String[]{"external-schema", "table-external-name"},
                null,
                "IMap",
                null,
                emptyList(),
                singletonMap("key", "value")
        );

        MappingsTable mappingTable = new MappingsTable(
                "catalog",
                null,
                "table-schema",
                singletonList(mapping),
                mock(SqlConnectorCache.class, RETURNS_MOCKS),
                (s) -> fail("Should not be invoked"), true);

        // when
        List<Object[]> rows = mappingTable.rows();

        // then
        assertThat(rows).containsExactly(new Object[]{
                "catalog"
                , "table-schema"
                , "table-name"
                , "\"external-schema\".\"table-external-name\""
                , "IMap"
                , "{}"
        });
    }

    @Test
    public void test_rows_dataconnection() {
        // given
        Mapping mapping = new Mapping(
                "table-name",
                new String[]{"external-schema", "table-external-name"},
                "some-dc",
                null,
                null,
                emptyList(),
                singletonMap("key", "value")
        );

        MappingsTable mappingTable = new MappingsTable(
                "catalog",
                null,
                "table-schema",
                singletonList(mapping),
                null,
                (dc) -> {
                    assertThat(dc).isEqualTo("some-dc");
                    return "external-dc-type";
                }, false);

        // when
        List<Object[]> rows = mappingTable.rows();

        // then
        assertThat(rows).containsExactly(new Object[]{
                "catalog"
                , "table-schema"
                , "table-name"
                , "\"external-schema\".\"table-external-name\""
                , "external-dc-type"
                , "{\"key\":\"value\"}"
        });
    }

    @Test
    public void test_rows_dataconnection_security_enabled() {
        final String dataConnectionName = "some-dc";
        final String dataConnectionType = "external-dc-type";

        // given
        Mapping mapping = new Mapping(
            "table-name",
            new String[]{"external-schema", "table-external-name"},
            dataConnectionName,
            null,
            null,
            emptyList(),
            singletonMap("key", "value")
        );

        SqlConnectorCache connectorCache = mock(SqlConnectorCache.class);
        SqlConnector connector = mock(SqlConnector.class);

        given(connectorCache.forType(dataConnectionType)).willReturn(connector);

        MappingsTable mappingTable = new MappingsTable(
            "catalog",
            null,
            "table-schema",
            singletonList(mapping),
            connectorCache,
            (dc) -> {
                assertThat(dc).isEqualTo(dataConnectionName);
                return dataConnectionType;
            },
            true
        );

        // when
        List<Object[]> rows = mappingTable.rows();

        // then
        assertThat(rows).containsExactly(new Object[]{
            "catalog"
            , "table-schema"
            , "table-name"
            , "\"external-schema\".\"table-external-name\""
            , "external-dc-type"
            , "{}"
        });

        verify(connectorCache).forType(dataConnectionType);
    }

    @Test
    public void test_rows_mapping_connector_type_data_connection_null() {
        Mapping mapping = new Mapping(
            "table-name",
            new String[]{"external-schema", "table-external-name"},
            null,
            null,
            null,
            emptyList(),
            singletonMap("key", "value")
        );

        SqlConnectorCache connectorCache = mock(SqlConnectorCache.class);

        MappingsTable mappingTable = new MappingsTable(
            "catalog",
            null,
            "table-schema",
            singletonList(mapping),
            connectorCache,
            (dc) -> null,
            true
        );

        assertThrows(IllegalStateException.class, mappingTable::rows);
        verifyNoInteractions(connectorCache);
    }

    @Test
    public void test_rows_mapping_data_connection_resolver_returns_null() {
        Mapping mapping = new Mapping(
            "table-name",
            new String[]{"external-schema", "table-external-name"},
            "foo",
            null,
            null,
            emptyList(),
            singletonMap("key", "value")
        );

        SqlConnectorCache connectorCache = mock(SqlConnectorCache.class);

        MappingsTable mappingTable = new MappingsTable(
            "catalog",
            null,
            "table-schema",
            singletonList(mapping),
            connectorCache,
            (dc) -> null,
            true
        );

        assertThrows(IllegalStateException.class, mappingTable::rows);
        verifyNoInteractions(connectorCache);
    }
}
