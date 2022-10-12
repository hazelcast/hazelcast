/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastore;

import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JdbcDataStoreFactoryTest {

    DataSource dataStore1;
    DataSource dataStore2;
    JdbcDataStoreFactory jdbcDataStoreFactory = new JdbcDataStoreFactory();

    @After
    public void tearDown() throws Exception {
        close(dataStore1);
        close(dataStore2);
        jdbcDataStoreFactory.close();
    }

    private static void close(DataSource dataStore) throws Exception {
        if (dataStore instanceof AutoCloseable) {
            ((AutoCloseable) dataStore).close();
        }
    }

    @Test
    public void should_return_same_datastore_when_shared() {
        JdbcDataStoreFactory jdbcDataStoreFactory = new JdbcDataStoreFactory();
        ExternalDataStoreConfig config = new ExternalDataStoreConfig()
                .setProperty("jdbcUrl", "jdbc:h2:mem:" + JdbcDataStoreFactoryTest.class.getSimpleName() + "_shared")
                .setShared(true);
        jdbcDataStoreFactory.init(config);

        dataStore1 = jdbcDataStoreFactory.getDataStore();
        dataStore2 = jdbcDataStoreFactory.getDataStore();

        assertThat(dataStore1).isNotNull();
        assertThat(dataStore2).isNotNull();
        assertThat(dataStore1).isSameAs(dataStore2);
    }

    @Test
    public void should_NOT_return_closing_datastore_when_shared() throws Exception {
        JdbcDataStoreFactory jdbcDataStoreFactory = new JdbcDataStoreFactory();
        ExternalDataStoreConfig config = new ExternalDataStoreConfig()
                .setProperty("jdbcUrl", "jdbc:h2:mem:" + JdbcDataStoreFactoryTest.class.getSimpleName() + "_shared")
                .setShared(true);
        jdbcDataStoreFactory.init(config);

        CloseableDataSource closeableDataSource = (CloseableDataSource) jdbcDataStoreFactory.getDataStore();
        closeableDataSource.close();

        ResultSet resultSet = executeQuery(closeableDataSource, "select 'some-name' as name");
        resultSet.next();
        String actualName = resultSet.getString(1);

        assertThat(actualName).isEqualTo("some-name");

    }

    @Test
    public void should_return_closing_datastore_when_not_shared() throws Exception {
        JdbcDataStoreFactory jdbcDataStoreFactory = new JdbcDataStoreFactory();
        ExternalDataStoreConfig config = new ExternalDataStoreConfig()
                .setProperty("jdbcUrl", "jdbc:h2:mem:" + JdbcDataStoreFactoryTest.class.getSimpleName() + "_shared")
                .setShared(false);
        jdbcDataStoreFactory.init(config);

        CloseableDataSource closeableDataSource = (CloseableDataSource) jdbcDataStoreFactory.getDataStore();
        closeableDataSource.close();

        assertThatThrownBy(() -> executeQuery(closeableDataSource, "select 'some-name' as name"))
                .isInstanceOf(SQLException.class)
                .hasMessageMatching("HikariDataSource HikariDataSource \\(HikariPool-\\d+\\) has been closed.");
    }

    private ResultSet executeQuery(CloseableDataSource closeableDataSource, String sql) throws SQLException {
        return closeableDataSource.getConnection().prepareStatement(sql).executeQuery();
    }

    @Test
    public void should_return_different_datastore_when_NOT_shared() {
        JdbcDataStoreFactory jdbcDataStoreFactory = new JdbcDataStoreFactory();
        ExternalDataStoreConfig config = new ExternalDataStoreConfig()
                .setProperty("jdbcUrl", "jdbc:h2:mem:" + JdbcDataStoreFactoryTest.class.getSimpleName() + "_not_shared")
                .setShared(false);
        jdbcDataStoreFactory.init(config);

        dataStore1 = jdbcDataStoreFactory.getDataStore();
        dataStore2 = jdbcDataStoreFactory.getDataStore();

        assertThat(dataStore1).isNotNull();
        assertThat(dataStore2).isNotNull();
        assertThat(dataStore1).isNotSameAs(dataStore2);
    }

    @Test
    public void should_close_shared_datasource_on_close() throws Exception {
        JdbcDataStoreFactory jdbcDataStoreFactory = new JdbcDataStoreFactory();
        ExternalDataStoreConfig config = new ExternalDataStoreConfig()
                .setProperty("jdbcUrl", "jdbc:h2:mem:" + JdbcDataStoreFactoryTest.class.getSimpleName() + "_shared")
                .setShared(true);
        jdbcDataStoreFactory.init(config);

        DataSource dataSource = jdbcDataStoreFactory.getDataStore();
        jdbcDataStoreFactory.close();

        assertThatThrownBy(() -> executeQuery(dataSource, "select 'some-name' as name"))
                .isInstanceOf(SQLException.class)
                .hasMessageMatching("HikariDataSource HikariDataSource \\(HikariPool-\\d+\\) has been closed.");
    }

    private ResultSet executeQuery(DataSource dataSource, String sql) throws SQLException {
        return dataSource.getConnection().prepareStatement(sql).executeQuery();
    }
}
