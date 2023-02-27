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

package com.hazelcast.datalink;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.datalink.impl.CloseableDataSource;
import com.hazelcast.jet.impl.connector.DataSourceFromConnectionSupplier;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.hazelcast.datalink.impl.HikariTestUtil.assertDataSourceClosed;
import static com.hazelcast.datalink.impl.HikariTestUtil.assertEventuallyNoHikariThreads;
import static com.hazelcast.datalink.impl.HikariTestUtil.assertPoolNameEndsWith;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JdbcDataLinkTest {

    private static final String TEST_CONFIG_NAME = JdbcDataLinkTest.class.getSimpleName();
    private static final String JDBC_URL_SHARED = "jdbc:h2:mem:" + JdbcDataLinkTest.class.getSimpleName() + "_shared";

    private static final DataLinkConfig SHARED_DATA_LINK_CONFIG = new DataLinkConfig()
            .setName(TEST_CONFIG_NAME)
            .setProperty("jdbcUrl", JDBC_URL_SHARED)
            .setShared(true);

    private static final DataLinkConfig NOT_SHARED_DATA_LINK_CONFIG = new DataLinkConfig()
            .setName(TEST_CONFIG_NAME)
            .setProperty("jdbcUrl", "jdbc:h2:mem:" + JdbcDataLinkTest.class.getSimpleName() + "_not_shared")
            .setShared(false);

    DataSource dataSource1;
    DataSource dataSource2;

    JdbcDataLink jdbcDataLink;

    @After
    public void tearDown() throws Exception {
        close(dataSource1);
        close(dataSource2);
        jdbcDataLink.close();
        assertEventuallyNoHikariThreads(TEST_CONFIG_NAME);
    }

    private static void close(DataSource dataSource) throws Exception {
        if (dataSource instanceof AutoCloseable) {
            ((AutoCloseable) dataSource).close();
        }
    }

    @Test
    public void should_return_same_data_link_when_shared() {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        dataSource1 = jdbcDataLink.getDataSource();
        dataSource2 = jdbcDataLink.getDataSource();

        assertThat(dataSource1).isNotNull();
        assertThat(dataSource2).isNotNull();
        assertThat(dataSource1).isSameAs(dataSource2);
    }

    @Test
    public void should_use_custom_hikari_pool_name() throws SQLException {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        dataSource1 = jdbcDataLink.getDataSource();
        assertPoolNameEndsWith(dataSource1, TEST_CONFIG_NAME);
    }

    @Test
    public void should_return_hikari_pool_when_shared() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        DataSource dataSource = jdbcDataLink.getDataSource();
        assertThat(dataSource).isInstanceOf(CloseableDataSource.class);
    }

    @Test
    public void should_return_single_use_data_source_when_NOT_shared() throws Exception {
        jdbcDataLink = new JdbcDataLink(NOT_SHARED_DATA_LINK_CONFIG);

        DataSource dataSource = jdbcDataLink.getDataSource();
        assertThat(dataSource).isInstanceOf(DataSourceFromConnectionSupplier.class);
    }

    @Test
    public void should_close_shared_datasource_when_data_source_and_link_closed() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);
        CloseableDataSource dataSource = (CloseableDataSource) jdbcDataLink.getDataSource();

        dataSource.close();
        jdbcDataLink.close();

        assertDataSourceClosed(dataSource, TEST_CONFIG_NAME);
    }

    /**
     * This test closes the data link and source in the opposite order then
     * {@link #should_close_shared_datasource_when_data_source_and_link_closed()}
     */
    @Test
    public void should_close_shared_datasource_when_data_link_and_source_closed() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        CloseableDataSource dataSource = (CloseableDataSource) jdbcDataLink.getDataSource();
        jdbcDataLink.close();

        String actualName;
        try (ResultSet resultSet = executeQuery(dataSource, "select 'some-name' as name")) {
            resultSet.next();
            actualName = resultSet.getString(1);
        }
        assertThat(actualName).isEqualTo("some-name");

        dataSource.close();
        assertDataSourceClosed(dataSource, TEST_CONFIG_NAME);
    }

    private ResultSet executeQuery(DataSource dataSource, String sql) throws SQLException {
        return dataSource.getConnection().prepareStatement(sql).executeQuery();
    }

    @Test
    public void list_resources_should_return_table() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        executeJdbc(JDBC_URL_SHARED, "CREATE TABLE MY_TABLE (ID INT, NAME VARCHAR)");

        List<Resource> resources = jdbcDataLink.listResources();
        assertThat(resources).contains(
                new Resource("BASE TABLE", "PUBLIC.MY_TABLE")
        );
    }

    @Test
    public void list_resources_should_return_table_in_schema() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        executeJdbc(JDBC_URL_SHARED, "CREATE SCHEMA MY_SCHEMA");
        executeJdbc(JDBC_URL_SHARED, "CREATE TABLE MY_SCHEMA.MY_TABLE (ID INT, NAME VARCHAR)");

        List<Resource> resources = jdbcDataLink.listResources();
        assertThat(resources).contains(
                new Resource("BASE TABLE", "MY_SCHEMA.MY_TABLE")
        );
    }

    @Test
    public void list_resources_should_return_view() throws Exception {
        jdbcDataLink = new JdbcDataLink(SHARED_DATA_LINK_CONFIG);

        executeJdbc(JDBC_URL_SHARED, "CREATE TABLE MY_TABLE (ID INT, NAME VARCHAR)");
        executeJdbc(JDBC_URL_SHARED, "CREATE VIEW MY_TABLE_VIEW AS SELECT * FROM MY_TABLE");

        List<Resource> resources = jdbcDataLink.listResources();
        assertThat(resources).contains(
                new Resource("VIEW", "PUBLIC.MY_TABLE_VIEW")
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
