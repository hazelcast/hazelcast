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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * Separate test class for SHOW DATA CONNECTIONS.
 * It is required due to inability to remove of config-created data connection.
 * Also, it is important for us to test ability of SHOW DATA CONNECTIONS
 * to show both sql-created and config-created data connections.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ShowDataConnectionsSqlTest extends SqlJsonTestSupport {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initializeWithClient(1, null, null);
    }

    @Test
    public void test_showDataConnections() {
        // when no data connections - empty list
        assertRowsOrdered("show data connections", emptyList());

        // given
        // create data connections via CONFIG
        getNodeEngineImpl(instance()).getDataConnectionService().createConfigDataConnection(
                new DataConnectionConfig("dl")
                        .setType("dummy")
        );

        // create data connections via SQL
        List<String> dlNames = IntStream.range(0, 5).mapToObj(i -> "dl" + i).collect(toList());
        for (String dlName : dlNames) {
            instance().getSql()
                    .execute("CREATE DATA CONNECTION " + dlName + " TYPE DUMMY SHARED OPTIONS ('b' = 'c')").close();
        }

        dlNames.add(0, "dl");

        // when & then
        List<Row> expectedRows = dlNames.stream()
                .map(name -> new Row(name, "dummy", jsonArray("testType1", "testType2")))
                .collect(toList());
        assertRowsOrdered(instance(), "show data connections", expectedRows);
        assertRowsOrdered(client(), "show data connections", expectedRows);
    }
}
