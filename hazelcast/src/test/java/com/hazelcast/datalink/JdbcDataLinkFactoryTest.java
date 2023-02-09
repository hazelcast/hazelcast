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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.hazelcast.datalink.impl.HikariTestUtil.assertDataSourceClosed;
import static com.hazelcast.datalink.impl.HikariTestUtil.assertEventuallyNoHikariThreads;
import static com.hazelcast.datalink.impl.HikariTestUtil.assertPoolNameEndsWith;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JdbcDataLinkFactoryTest {

    private static final String TEST_CONFIG_NAME = JdbcDataLinkFactoryTest.class.getSimpleName();
    private static final DataLinkConfig SHARED_DATA_LINK_CONFIG = new DataLinkConfig()
            .setName(TEST_CONFIG_NAME)
            .setProperty("jdbcUrl", "jdbc:h2:mem:" + JdbcDataLinkFactoryTest.class.getSimpleName() + "_shared")
            .setShared(true);
    private static final DataLinkConfig NOT_SHARED_DATA_LINK_CONFIG = new DataLinkConfig()
            .setName(TEST_CONFIG_NAME)
            .setProperty("jdbcUrl", "jdbc:h2:mem:" + JdbcDataLinkFactoryTest.class.getSimpleName() + "_not_shared")
            .setShared(false);
    DataSource dataSource1;
    DataSource dataSource2;
    JdbcDataLinkFactory jdbcDataLinkFactory = new JdbcDataLinkFactory();

    @After
    public void tearDown() throws Exception {
        close(dataSource1);
        close(dataSource2);
        jdbcDataLinkFactory.close();
        assertEventuallyNoHikariThreads(TEST_CONFIG_NAME);
    }

    private static void close(DataSource dataSource) throws Exception {
        if (dataSource instanceof AutoCloseable) {
            ((AutoCloseable) dataSource).close();
        }
    }

    @Test
    public void should_return_TRUE_when_connection_is_ok() throws Exception {
        jdbcDataLinkFactory.init(SHARED_DATA_LINK_CONFIG);

        assertThat(jdbcDataLinkFactory.testConnection()).isTrue();
    }

    @Test
    public void should_throw_when_data_link_is_closed() throws Exception {
        jdbcDataLinkFactory.init(SHARED_DATA_LINK_CONFIG);

        jdbcDataLinkFactory.close();

        assertThatThrownBy(() -> jdbcDataLinkFactory.testConnection())
                .isExactlyInstanceOf(SQLException.class)
                .hasMessageContaining("has been closed");
    }

    @Test
    public void should_return_same_datastore_when_shared() {
        jdbcDataLinkFactory.init(SHARED_DATA_LINK_CONFIG);

        dataSource1 = jdbcDataLinkFactory.getDataLink();
        dataSource2 = jdbcDataLinkFactory.getDataLink();

        assertThat(dataSource1).isNotNull();
        assertThat(dataSource2).isNotNull();
        assertThat(dataSource1).isSameAs(dataSource2);
    }

    @Test
    public void should_use_custom_hikari_pool_name() throws SQLException {
        jdbcDataLinkFactory.init(SHARED_DATA_LINK_CONFIG);

        dataSource1 = jdbcDataLinkFactory.getDataLink();
        assertPoolNameEndsWith(dataSource1, TEST_CONFIG_NAME);
    }

    @Test
    public void should_NOT_return_closing_datastore_when_shared() throws Exception {
        jdbcDataLinkFactory.init(SHARED_DATA_LINK_CONFIG);

        CloseableDataSource closeableDataSource = (CloseableDataSource) jdbcDataLinkFactory.getDataLink();
        closeableDataSource.close();

        ResultSet resultSet = executeQuery(closeableDataSource, "select 'some-name' as name");
        resultSet.next();
        String actualName = resultSet.getString(1);

        assertThat(actualName).isEqualTo("some-name");
    }

    @Test
    public void should_return_closing_datastore_when_not_shared() throws Exception {
        jdbcDataLinkFactory.init(NOT_SHARED_DATA_LINK_CONFIG);

        CloseableDataSource closeableDataSource = (CloseableDataSource) jdbcDataLinkFactory.getDataLink();
        closeableDataSource.close();

        assertDataSourceClosed(closeableDataSource, TEST_CONFIG_NAME);
    }

    private ResultSet executeQuery(CloseableDataSource closeableDataSource, String sql) throws SQLException {
        return closeableDataSource.getConnection().prepareStatement(sql).executeQuery();
    }

    @Test
    public void should_return_different_datastore_when_NOT_shared() {
        jdbcDataLinkFactory.init(NOT_SHARED_DATA_LINK_CONFIG);

        dataSource1 = jdbcDataLinkFactory.getDataLink();
        dataSource2 = jdbcDataLinkFactory.getDataLink();

        assertThat(dataSource1).isNotNull();
        assertThat(dataSource2).isNotNull();
        assertThat(dataSource1).isNotSameAs(dataSource2);
    }

    @Test
    public void should_close_shared_datasource_on_close() throws Exception {
        jdbcDataLinkFactory.init(SHARED_DATA_LINK_CONFIG);

        DataSource dataSource = jdbcDataLinkFactory.getDataLink();
        jdbcDataLinkFactory.close();

        assertDataSourceClosed(dataSource, TEST_CONFIG_NAME);
    }

}
