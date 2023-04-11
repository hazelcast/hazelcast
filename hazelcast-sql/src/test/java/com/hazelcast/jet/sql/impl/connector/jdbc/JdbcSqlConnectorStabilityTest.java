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
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

import static com.hazelcast.function.ConsumerEx.noop;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;

/**
 * This test should not be subclassed by a DB Provider that uses TestContainers underneath.
 * Because when H2 is shut down, it will not re-start when we try to get a new JDBC connection
 * <p>
 * But TestContainers based DB providers such a MySQLDatabaseProvider
 * will automatically re-start the container. So it is not possible to shut them down
 */
public class JdbcSqlConnectorStabilityTest extends JdbcSqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Test
    public void dataConnectionDownShouldTimeout() throws SQLException {
        String tableName = "table1";
        createTable(tableName);
        insertItems(tableName, 5);

        execute(
                "CREATE MAPPING " + tableName + " ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        // Shut down H2 for the entire test suite
        databaseProvider.shutdown();

        // We should not be able to access H2 anymore
        assertThatThrownBy(() -> {
            sqlService
                    .execute("SELECT * FROM " + tableName)
                    .forEach(noop());
        }).isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Mapping 'table1' is invalid: com.hazelcast.core.HazelcastException: " +
                                      "Could not determine dialect for data connection: testDatabaseRef");
    }

    @Test
    public void dataConnectionDownShouldNotAffectUnrelatedQueries() {
        // Shut down H2 for the entire test suite
        databaseProvider.shutdown();

        // Still we should be able to read from generated table
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
