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

import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.OrderWith;
import org.junit.runner.manipulation.Alphanumeric;

import java.sql.SQLException;
import java.util.Properties;

import static com.hazelcast.function.ConsumerEx.noop;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;

// The order of tests is important
@OrderWith(Alphanumeric.class)
public class JdbcSqlConnectorStabilityTest extends JdbcSqlTestSupport {

    private static H2DatabaseProvider h2DatabaseProvider;

    protected static void initialize() {
        Config config = smallInstanceConfig();
        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", dbConnectionUrl);
        config.addDataConnectionConfig(
                new DataConnectionConfig(TEST_DATABASE_REF)
                        .setType("jdbc")
                        .setProperties(properties));

        initialize(2, config);
        sqlService = instance().getSql();
    }

    // Extending test class must override this method
    protected void stopDatabase() {
        h2DatabaseProvider.shutdown();
        h2DatabaseProvider = null;
    }

    @BeforeClass
    public static void beforeClass() {
        h2DatabaseProvider = new H2DatabaseProvider();
        dbConnectionUrl = h2DatabaseProvider.createDatabase(JdbcSqlTestSupport.class.getName());
        initialize();
    }

    @Test
    public void a_dataConnectionDownShouldTimeout() throws SQLException {
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

        stopDatabase();

        // We should not be able to access DB anymore
        assertThatThrownBy(() -> {
            sqlService
                    .execute("SELECT * FROM " + tableName)
                    .forEach(noop());
        }).isInstanceOf(HazelcastSqlException.class);
    }

    @Test
    public void b_dataConnectionDownShouldNotAffectUnrelatedQueries() {
        // We should be able to read from generated table even if the DB is stopped
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
