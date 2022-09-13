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

package com.hazelcast.jet.sql;

import com.hazelcast.config.Config;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlMemoryManagementTest extends SqlTestSupport {

    private static final int MAX_PROCESSOR_ACCUMULATED_RECORDS = 2;

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        Config config = smallInstanceConfig();
        config.getJetConfig()
                .setCooperativeThreadCount(1)
                .setMaxProcessorAccumulatedRecords(MAX_PROCESSOR_ACCUMULATED_RECORDS);

        initialize(2, config);
        sqlService = instance().getSql();
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsExceededWhileInserting_then_throws() {
        String name = randomName();
        createMapping(name, Integer.class, String.class);

        assertThatThrownBy(() -> sqlService.execute("INSERT INTO " + name + " VALUES (0, '0'), (1, '1'), (2, '2')"))
                .hasMessageContaining("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsExceededWhileGrouping_then_throws() {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                singletonList("name"),
                singletonList(VARCHAR),
                asList(new String[]{"Alice"}, new String[]{"Bob"}, new String[]{"Joe"})
        );

        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " + name + " GROUP BY name").iterator().next())
                .hasMessageContaining("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsExceededWhileSorting_then_throws() {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                singletonList("name"),
                singletonList(VARCHAR),
                asList(new String[]{"Alice"}, new String[]{"Bob"}, new String[]{"Joe"})
        );

        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " + name + " ORDER BY name").iterator().next())
                .hasMessageContaining("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsExceededWhileS2SJoin_then_throws() {
        String left = randomName();
        String right = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                left,
                singletonList("ts"),
                singletonList(BIGINT),
                row(1L),
                row(1L),
                row(1L)
        );

        TestStreamSqlConnector.create(
                sqlService,
                right,
                singletonList("ts"),
                singletonList(BIGINT),
                row(1L),
                row(1L),
                row(1L)
        );

        sqlService.execute("CREATE VIEW s1 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + left + " , DESCRIPTOR(ts), 10))");
        sqlService.execute("CREATE VIEW s2 AS " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + right + ", DESCRIPTOR(ts), 10))");

        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM s1 JOIN s2 ON s2.ts = s1.ts").iterator().next())
                .hasMessageContaining("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
    }
}
