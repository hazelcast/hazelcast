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

package com.hazelcast.datalink.impl;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.datalink.DataLinkResource;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static com.hazelcast.datalink.impl.HikariTestUtil.assertEventuallyNoHikariThreads;
import static com.hazelcast.datalink.impl.HikariTestUtil.assertPoolNameEndsWith;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JdbcDataLinkTest {

    private static final String TEST_CONFIG_NAME = JdbcDataLinkTest.class.getSimpleName();
    private static final String JDBC_URL_SHARED = "jdbc:h2:mem:" + JdbcDataLinkTest.class.getSimpleName() + "_shared";

    private static final DataLinkConfig SHARED_DATA_LINK_CONFIG = new DataLinkConfig()
            .setName(TEST_CONFIG_NAME)
            .setProperty("jdbcUrl", JDBC_URL_SHARED)
            .setShared(true);

    private static final DataLinkConfig SINGLE_USE_DATA_LINK_CONFIG = new DataLinkConfig()
            .setName(TEST_CONFIG_NAME)
            .setProperty("jdbcUrl", "jdbc:h2:mem:" + JdbcDataLinkTest.class.getSimpleName() + "_single_use")
            .setShared(false);

    Connection connection1;
    Connection connection2;

    JdbcDataLink jdbcDataLink;

    @After
    public void tearDown() throws Exception {
        close(connection1);
        close(connection2);
        jdbcDataLink.release();
        assertEventuallyNoHikariThreads(TEST_CONFIG_NAME);
    }

    private static void close(Connection connection) throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void should_return_name() {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);
        assertThat(jdbcDataLink.getName()).isEqualTo(TEST_CONFIG_NAME);
    }

    @Test
    public void should_return_config() {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        DataLinkConfig config = jdbcDataLink.getConfig();

        assertThat(config).isSameAs(SHARED_DATA_LINK_CONFIG);
    }

    @Test
    public void should_return_options() {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        Map<String, String> options = jdbcDataLink.options();
        assertThat(options).containsExactly(
                entry("jdbcUrl", JDBC_URL_SHARED)
        );
    }

    @Test
    public void should_return_same_connection_when_shared() throws Exception {
        DataLinkConfig config = new DataLinkConfig(SHARED_DATA_LINK_CONFIG)
                .setProperty("maximumPoolSize", "1");

        jdbcDataLink = new JdbcDataLink(config);

        connection1 = jdbcDataLink.getConnection();
        assertThat(connection1).isNotNull();
        Connection unwrapped1 = connection1.unwrap(Connection.class);

        connection1.close();

        // used maximumPoolSize above, after closing it should return same connection
        connection2 = jdbcDataLink.getConnection();
        assertThat(connection2).isNotNull();
        Connection unwrapped2 = connection2.unwrap(Connection.class);

        assertThat(unwrapped1).isSameAs(unwrapped2);
    }

    @Test
    public void should_use_custom_hikari_pool_name() throws SQLException {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);
        HikariDataSource pool = jdbcDataLink.pooledDataSource();

        assertPoolNameEndsWith(pool, TEST_CONFIG_NAME);
    }

    @Test
    public void should_return_hikari_connection_when_shared() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        try (ConnectionDelegate connection = (ConnectionDelegate) jdbcDataLink.getConnection()) {
            assertThat(connection.getDelegate()).isInstanceOf(HikariProxyConnection.class);
        }
    }

    @Test
    public void should_return_h2_jdbc_connection_when_NOT_shared() throws Exception {
        jdbcDataLink = new JdbcDataLink(SINGLE_USE_DATA_LINK_CONFIG);

        try (Connection connection = jdbcDataLink.getConnection()) {
            assertThat(connection).isInstanceOf(JdbcConnection.class);
        }
    }

    @Test
    public void should_close_shared_pool_when_connection_and_data_link_closed() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);
        HikariDataSource pool = jdbcDataLink.pooledDataSource();

        Connection connection = jdbcDataLink.getConnection();

        connection.close();
        jdbcDataLink.release();

        assertThat(pool.isClosed())
                .describedAs("Connection pool should have been closed")
                .isTrue();
    }

    /**
     * This test closes the data link and source in the opposite order then
     * {@link #should_close_shared_pool_when_connection_and_data_link_closed()}
     */
    @Test
    public void should_close_shared_pool_when_data_link_and_connection_closed() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);
        HikariDataSource pool = jdbcDataLink.pooledDataSource();

        try (Connection connection = jdbcDataLink.getConnection()) {
            jdbcDataLink.release();

            assertThat(pool.isClosed())
                    .describedAs("Connection pool should be still open")
                    .isFalse();
        }

        assertThat(pool.isClosed())
                .describedAs("Connection pool should have been closed")
                .isTrue();
    }

    @Test
    public void list_resources_should_return_table() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        executeJdbc(JDBC_URL_SHARED, "CREATE TABLE MY_TABLE (ID INT, NAME VARCHAR)");

        List<DataLinkResource> dataLinkResources = jdbcDataLink.listResources();
        assertThat(dataLinkResources).contains(
                new DataLinkResource("TABLE", "PUBLIC.MY_TABLE")
        );
    }

    @Test
    public void list_resources_should_return_table_in_schema() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        executeJdbc(JDBC_URL_SHARED, "CREATE SCHEMA MY_SCHEMA");
        executeJdbc(JDBC_URL_SHARED, "CREATE TABLE MY_SCHEMA.MY_TABLE (ID INT, NAME VARCHAR)");

        List<DataLinkResource> dataLinkResources = jdbcDataLink.listResources();
        assertThat(dataLinkResources).contains(
                new DataLinkResource("TABLE", "MY_SCHEMA.MY_TABLE")
        );
    }

    @Test
    public void list_resources_should_return_view() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        executeJdbc(JDBC_URL_SHARED, "CREATE TABLE MY_TABLE (ID INT, NAME VARCHAR)");
        executeJdbc(JDBC_URL_SHARED, "CREATE VIEW MY_TABLE_VIEW AS SELECT * FROM MY_TABLE");

        List<DataLinkResource> dataLinkResources = jdbcDataLink.listResources();
        assertThat(dataLinkResources).contains(
                new DataLinkResource("TABLE", "PUBLIC.MY_TABLE_VIEW")
        );
    }

    public static void executeJdbc(String url, String sql) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(sql);
        }
    }

}
