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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.mock.MockUtil;
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector.OPTION_BOOTSTRAP_SERVERS;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataConnectionsTableTest extends SimpleTestInClusterSupport {
    @Mock
    SqlConnectorCache connectorCache;

    private AutoCloseable openMocks;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Before
    public void before() throws Exception {
        openMocks = openMocks(this);
    }

    @After
    public void tearDown() throws Exception {
        super.shutdownFactory();
        MockUtil.closeMocks(openMocks);
    }

    @Test
    public void test_rows() {
        // given
        DataConnectionCatalogEntry dc = new DataConnectionCatalogEntry(
                "dc-name",
                "dc-type",
                false,
                singletonMap("key", "value")
        );

        DataConnectionsTable dcTable = new DataConnectionsTable(
                "catalog",
                "public",
                "dc-schema",
                singletonList(dc),
                connectorCache,
                false);

        // when
        List<Object[]> rows = dcTable.rows();

        // then
        assertThat(rows).containsExactly(new Object[]{
                "catalog"
                , "dc-schema"
                , "dc-name"
                , "dc-type"
                , false
                , "{\"key\":\"value\"}"
                , "SQL"
        });
    }

    @Test
    public void test_rows_enabledSecurity() {
        when(connectorCache.forType("Kafka")).thenReturn(new KafkaSqlConnector());

        // given
        DataConnectionCatalogEntry dc = new DataConnectionCatalogEntry(
                "dc-name",
                "Kafka",
                false,
                Map.of(OPTION_BOOTSTRAP_SERVERS, "value", "password", "secret")
        );

        DataConnectionsTable dcTable = new DataConnectionsTable(
                "catalog",
                "public",
                "dc-schema",
                singletonList(dc),
                connectorCache,
                true);

        // when
        List<Object[]> rows = dcTable.rows();

        // then
        assertThat(rows).containsExactly(new Object[]{
                "catalog"
                , "dc-schema"
                , "dc-name"
                , "Kafka"
                , false
                , "{\"" + OPTION_BOOTSTRAP_SERVERS + "\":\"value\"}"
                , "SQL"
        });
    }
}
