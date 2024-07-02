/*
 * Copyright 2024 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;

public class SelectJdbcSqlConnectorParallelTest extends JdbcSqlTestSupport {

    private static final int ITEM_COUNT = 5;
    private static final int THREAD_COUNT = 4;

    private String tableName;

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, ITEM_COUNT);

        execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        );
    }

    /**
     * reproducer for https://hazelcast.atlassian.net/browse/ESC-26
     */
    @Test
    public void selectAllFromTableWhereIdColumnParallel() throws Exception {
        int repeatCount = 10000;
        Future<?>[] futures = new Future[THREAD_COUNT];
        var pool = Executors.newFixedThreadPool(THREAD_COUNT);
        try {
            for (int i = 0; i < THREAD_COUNT; ++i) {
                futures[i] = pool.submit(() -> {
                    for (int j = 0; j < repeatCount; ++j) {
                        int key = ThreadLocalRandom.current().nextInt(ITEM_COUNT);
                        assertRowsAnyOrder(
                                "SELECT * FROM " + tableName + " WHERE id = ?",
                                newArrayList(key),
                                newArrayList(
                                        new Row(key, "name-" + key)
                                )
                        );
                    }
                });
            }
        } finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        }

        for (var f : futures) {
            assertThat(f).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
        }
    }

}
