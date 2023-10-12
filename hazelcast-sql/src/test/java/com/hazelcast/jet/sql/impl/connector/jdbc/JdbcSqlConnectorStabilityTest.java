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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import com.hazelcast.test.jdbc.TestDatabaseProvider;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.function.ConsumerEx.noop;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;

public class JdbcSqlConnectorStabilityTest extends JdbcSqlTestSupport {

    protected static String tableName;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initializeStabilityTest(new H2DatabaseProvider());
    }

    public static void initializeStabilityTest(TestDatabaseProvider provider) throws Exception {
        initialize(provider);

        tableName = randomTableName();
        createTable(tableName);
        insertItems(tableName, 5);

        execute(
                "CREATE MAPPING " + tableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        databaseProvider.shutdown();
    }

    // We should not be able to access DB anymore
    @Test
    public void dataConnectionDownShouldTimeout() {
        assertThatThrownBy(() -> {
            sqlService
                    .execute("SELECT * FROM " + tableName)
                    .forEach(noop());
        }).isInstanceOf(HazelcastSqlException.class);
    }

    // We should be able to read from generated table even if the DB is stopped
    @Test
    public void dataConnectionDownShouldNotAffectUnrelatedQueries() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(generate_series(0, 4))",
                newArrayList(
                        new Row(0),
                        new Row(1),
                        new Row(2),
                        new Row(3),
                        new Row(4)
                )
        );
    }
}
