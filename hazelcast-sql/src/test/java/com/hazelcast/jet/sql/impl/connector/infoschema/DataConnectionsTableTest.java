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

import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataConnectionsTableTest {

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
                true);

        // when
        List<Object[]> rows = dcTable.rows();

        // then
        assertThat(rows).containsExactly(new Object[]{
                "catalog"
                , "dc-schema"
                , "dc-name"
                , "dc-type"
                , false
                , null
                , "SQL"
        });
    }
}
