/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.dataconnection.impl;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import org.h2.jdbc.JdbcConnection;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.hazelcast.dataconnection.impl.HikariTestUtil.assertEventuallyNoHikariThreads;
import static com.hazelcast.dataconnection.impl.HikariTestUtil.assertPoolNameEndsWith;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JdbcDataConnectionTest {

    private static final String TEST_NAME = JdbcDataConnectionTest.class.getSimpleName();
    public static final String DB_NAME_SHARED = (TEST_NAME + "_shared").toUpperCase(Locale.ROOT);
    private static final String JDBC_URL_SHARED = "jdbc:h2:mem:"
            + DB_NAME_SHARED
            + ";DB_CLOSE_DELAY=-1";

    private static final DataConnectionConfig SHARED_DATA_CONNECTION_CONFIG = new DataConnectionConfig()
            .setName(TEST_NAME)
            .setProperty("jdbcUrl", JDBC_URL_SHARED)
            .setShared(true);

    private static final DataConnectionConfig SINGLE_USE_DATA_CONNECTION_CONFIG = new DataConnectionConfig()
            .setName(TEST_NAME)
            .setProperty("jdbcUrl", "jdbc:h2:mem:" + TEST_NAME + "_single_use")
            .setShared(false);

    Connection connection1;
    Connection connection2;

    JdbcDataConnection jdbcDataConnection;

    @After
    public void tearDown() throws Exception {
        close(connection1);
        close(connection2);
        jdbcDataConnection.release();
        assertEventuallyNoHikariThreads(TEST_NAME);
        executeJdbc(JDBC_URL_SHARED, "shutdown");
    }

    private static void close(Connection connection) throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void should_return_name() {
        jdbcDataConnection = new JdbcDataConnection(SHARED_DATA_CONNECTION_CONFIG);
        assertThat(jdbcDataConnection.getName()).isEqualTo(TEST_NAME);
    }

    @Test
    public void should_return_config() {
        jdbcDataConnection = new JdbcDataConnection(SHARED_DATA_CONNECTION_CONFIG);

        DataConnectionConfig config = jdbcDataConnection.getConfig();

        assertThat(config).isSameAs(SHARED_DATA_CONNECTION_CONFIG);
    }

    @Test
    public void should_return_options() {
        jdbcDataConnection = new JdbcDataConnection(SHARED_DATA_CONNECTION_CONFIG);

        Map<String, String> options = jdbcDataConnection.options();
        assertThat(options).containsExactly(
                entry("jdbcUrl", JDBC_URL_SHARED)
        );
    }

    @Test
    public void should_return_same_connection_when_shared() throws Exception {
        DataConnectionConfig config = new DataConnectionConfig(SHARED_DATA_CONNECTION_CONFIG)
                .setProperty("maximumPoolSize", "1");

        jdbcDataConnection = new JdbcDataConnection(config);

        connection1 = jdbcDataConnection.getConnection();
        assertThat(connection1).isNotNull();
        Connection unwrapped1 = connection1.unwrap(Connection.class);

        connection1.close();

        // used maximumPoolSize above, after closing it should return same connection
        connection2 = jdbcDataConnection.getConnection();
        assertThat(connection2).isNotNull();
        Connection unwrapped2 = connection2.unwrap(Connection.class);

        assertThat(unwrapped1).isSameAs(unwrapped2);
    }

    @Test
    public void should_use_custom_hikari_pool_name() throws SQLException {
        jdbcDataConnection = new JdbcDataConnection(SHARED_DATA_CONNECTION_CONFIG);
        HikariDataSource pool = jdbcDataConnection.pooledDataSource();

        assertPoolNameEndsWith(pool, TEST_NAME);
    }

    @Test
    public void should_return_hikari_connection_when_shared() throws Exception {
        jdbcDataConnection = new JdbcDataConnection(SHARED_DATA_CONNECTION_CONFIG);

        try (ConnectionDelegate connection = (ConnectionDelegate) jdbcDataConnection.getConnection()) {
            assertThat(connection.getDelegate()).isInstanceOf(HikariProxyConnection.class);
        }
    }

    @Test
    public void should_return_h2_jdbc_connection_when_NOT_shared() throws Exception {
        jdbcDataConnection = new JdbcDataConnection(SINGLE_USE_DATA_CONNECTION_CONFIG);

        try (Connection connection = jdbcDataConnection.getConnection()) {
            assertThat(connection).isInstanceOf(JdbcConnection.class);
        }
    }

    @Test
    public void should_close_shared_pool_when_connection_and_data_connection_closed() throws Exception {
        jdbcDataConnection = new JdbcDataConnection(SHARED_DATA_CONNECTION_CONFIG);
        HikariDataSource pool = jdbcDataConnection.pooledDataSource();

        Connection connection = jdbcDataConnection.getConnection();

        connection.close();
        jdbcDataConnection.release();

        assertThat(pool.isClosed())
                .describedAs("Connection pool should have been closed")
                .isTrue();
    }

    /**
     * This test closes the data connection and source in the opposite order then
     * {@link #should_close_shared_pool_when_connection_and_data_connection_closed()}
     */
    @Test
    public void should_close_shared_pool_when_data_connection_and_connection_closed() throws Exception {
        jdbcDataConnection = new JdbcDataConnection(SHARED_DATA_CONNECTION_CONFIG);
        HikariDataSource pool = jdbcDataConnection.pooledDataSource();

        try (Connection connection = jdbcDataConnection.getConnection()) {
            jdbcDataConnection.release();

            assertThat(pool.isClosed())
                    .describedAs("Connection pool should be still open")
                    .isFalse();
        }

        assertThat(pool.isClosed())
                .describedAs("Connection pool should have been closed")
                .isTrue();
    }

    @Test
    public void shared_connection_should_be_initialized_lazy() {
        jdbcDataConnection = new JdbcDataConnection(new DataConnectionConfig()
                .setName(TEST_NAME)
                .setProperty("jdbcUrl", "invalid-jdbc-url")
                .setShared(true));

        assertThatThrownBy(() -> jdbcDataConnection.getConnection())
                .hasRootCauseInstanceOf(SQLException.class)
                .hasRootCauseMessage("No suitable driver");
    }

    @Test
    public void list_resources_should_return_table() throws Exception {
        jdbcDataConnection = new JdbcDataConnection(SHARED_DATA_CONNECTION_CONFIG);

        executeJdbc(JDBC_URL_SHARED, "CREATE TABLE MY_TABLE (ID INT, NAME VARCHAR)");

        List<DataConnectionResource> dataConnectionResources = jdbcDataConnection.listResources();
        assertThat(dataConnectionResources).contains(
                new DataConnectionResource("TABLE", DB_NAME_SHARED, "PUBLIC", "MY_TABLE")
        );
    }

    @Test
    public void list_resources_should_return_table_in_schema() throws Exception {
        jdbcDataConnection = new JdbcDataConnection(SHARED_DATA_CONNECTION_CONFIG);

        executeJdbc(JDBC_URL_SHARED, "CREATE SCHEMA MY_SCHEMA");
        executeJdbc(JDBC_URL_SHARED, "CREATE TABLE MY_SCHEMA.MY_TABLE (ID INT, NAME VARCHAR)");

        List<DataConnectionResource> dataConnectionResources = jdbcDataConnection.listResources();
        assertThat(dataConnectionResources).contains(
                new DataConnectionResource("TABLE", DB_NAME_SHARED, "MY_SCHEMA", "MY_TABLE")
        );
    }

    @Test
    public void list_resources_should_return_view() throws Exception {
        jdbcDataConnection = new JdbcDataConnection(SHARED_DATA_CONNECTION_CONFIG);

        executeJdbc(JDBC_URL_SHARED, "CREATE TABLE MY_TABLE (ID INT, NAME VARCHAR)");
        executeJdbc(JDBC_URL_SHARED, "CREATE VIEW MY_TABLE_VIEW AS SELECT * FROM MY_TABLE");

        List<DataConnectionResource> dataConnectionResources = jdbcDataConnection.listResources();
        assertThat(dataConnectionResources).contains(
                new DataConnectionResource("TABLE", DB_NAME_SHARED, "PUBLIC", "MY_TABLE_VIEW")
        );
    }

    @Test
    public void should_list_resource_types() {
        // given
        jdbcDataConnection = new JdbcDataConnection(SHARED_DATA_CONNECTION_CONFIG);

        // when
        Collection<String> resourcedTypes = jdbcDataConnection.resourceTypes();

        //then
        assertThat(resourcedTypes)
                .map(r -> r.toLowerCase(Locale.ROOT))
                .containsExactlyInAnyOrder("table");
    }

    public static void executeJdbc(String url, String sql) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(sql);
        }
    }

    public static DataSource pooledDataSource(JdbcDataConnection dataConnection) {
        return dataConnection.pooledDataSource();
    }

    public static boolean isClosed(DataSource dataSource) {
        try {
            return dataSource.unwrap(HikariDataSource.class).isClosed();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
