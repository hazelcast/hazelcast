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
package com.hazelcast.datastore.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.datastore.ExternalDataStoreFactory;
import com.hazelcast.datastore.ExternalDataStoreService;
import com.hazelcast.datastore.JdbcDataStoreFactory;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.hazelcast.datastore.impl.HikariTestUtil.assertDataSourceClosed;
import static com.hazelcast.datastore.impl.HikariTestUtil.assertEventuallyNoHikariThreads;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExternalDataStoreServiceImplTest extends HazelcastTestSupport {
    private static final String TEST_CONFIG_NAME = ExternalDataStoreServiceImplTest.class.getSimpleName();

    private final Config config = smallInstanceConfig();
    private final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private ExternalDataStoreService externalDataStoreService;

    @Before
    public void configure() {
        ExternalDataStoreConfig externalDataStoreConfig = new ExternalDataStoreConfig()
                .setName(TEST_CONFIG_NAME)
                .setClassName("com.hazelcast.datastore.JdbcDataStoreFactory")
                .setProperty("jdbcUrl", "jdbc:h2:mem:" + ExternalDataStoreServiceImplTest.class.getSimpleName());
        config.addExternalDataStoreConfig(externalDataStoreConfig);
        externalDataStoreService = getExternalDataStoreService();
    }

    @After
    public void tearDown() throws Exception {
        hazelcastInstanceFactory.shutdownAll();
        assertEventuallyNoHikariThreads(TEST_CONFIG_NAME);
    }

    @Test
    public void test_connection_should_return_TRUE_when_datastore_exists() throws Exception {
        ExternalDataStoreConfig externalDSConfig = config.getExternalDataStoreConfig(TEST_CONFIG_NAME);

        assertThat(externalDataStoreService.testConnection(externalDSConfig)).isTrue();
    }

    @Test
    public void test_connection_should_throw_when_no_db_is_present() {
        ExternalDataStoreConfig externalDSConfig = new ExternalDataStoreConfig()
                .setName(TEST_CONFIG_NAME)
                .setClassName("com.hazelcast.datastore.JdbcDataStoreFactory")
                .setProperty("jdbcUrl", "jdbc:h2:file:/random/path;IFEXISTS=TRUE");

        assertThatThrownBy(() -> externalDataStoreService.testConnection(externalDSConfig))
                .hasMessageContaining("Database \"/random/path\" not found");
    }

    @Test
    public void test_connection_should_throw_when_no_suitable_driver_is_present() {
        ExternalDataStoreConfig externalDSConfig = new ExternalDataStoreConfig()
                .setName(TEST_CONFIG_NAME)
                .setClassName("com.hazelcast.datastore.JdbcDataStoreFactory")
                .setProperty("jdbcUrl", "jdbc:mysql:localhost:3306/random_db");


        assertThatThrownBy(() -> externalDataStoreService.testConnection(externalDSConfig))
                .isExactlyInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(SQLException.class)
                .hasRootCauseMessage("No suitable driver");
    }

    @Test
    public void should_return_working_datastore() throws Exception {
        ExternalDataStoreFactory<?> dataStoreFactory = externalDataStoreService.getExternalDataStoreFactory(TEST_CONFIG_NAME);
        assertInstanceOf(JdbcDataStoreFactory.class, dataStoreFactory);

        DataSource dataSource = ((JdbcDataStoreFactory) dataStoreFactory).getDataStore();

        String actualName;
        try (ResultSet resultSet = executeQuery(dataSource, "select 'some-name' as name")) {
            resultSet.next();
            actualName = resultSet.getString(1);
        }

        assertThat(actualName).isEqualTo("some-name");
    }

    @Test
    public void should_fail_when_non_existing_datastore() {
        assertThatThrownBy(() -> externalDataStoreService.getExternalDataStoreFactory("non-existing-data-store"))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("External data store factory 'non-existing-data-store' not found");
    }

    @Test
    public void should_close_factories() {
        ExternalDataStoreFactory<?> dataStoreFactory = externalDataStoreService.getExternalDataStoreFactory(TEST_CONFIG_NAME);

        DataSource dataSource = ((JdbcDataStoreFactory) dataStoreFactory).getDataStore();
        externalDataStoreService.close();

        assertDataSourceClosed(dataSource, TEST_CONFIG_NAME);
    }

    private ExternalDataStoreService getExternalDataStoreService() {
        HazelcastInstance instance = hazelcastInstanceFactory.newHazelcastInstance(config);
        return Util.getNodeEngine(instance).getExternalDataStoreService();
    }

    private ResultSet executeQuery(DataSource dataSource, String sql) throws SQLException {
        return dataSource.getConnection().prepareStatement(sql).executeQuery();
    }

    @Test
    public void should_fail_when_datastore_class_DOES_NOT_implements_ExternalDataStoreFactory() {
        Config wrongConfig = smallInstanceConfig();

        ExternalDataStoreConfig externalDataStoreConfig = new ExternalDataStoreConfig()
                .setName("wrong-class-name")
                .setClassName("java.lang.Object");
        wrongConfig.addExternalDataStoreConfig(externalDataStoreConfig);

        assertThatThrownBy(() -> hazelcastInstanceFactory.newHazelcastInstance(wrongConfig))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("External data store 'wrong-class-name' misconfigured: 'java.lang.Object'"
                        + " must implement 'com.hazelcast.datastore.ExternalDataStoreFactory'");
    }

    @Test
    public void should_fail_when_datastore_class_NON_existing() {
        Config wrongConfig = smallInstanceConfig();

        ExternalDataStoreConfig externalDataStoreConfig = new ExternalDataStoreConfig()
                .setName("non-existing-class")
                .setClassName("com.example.NonExistingClass");
        wrongConfig.addExternalDataStoreConfig(externalDataStoreConfig);

        assertThatThrownBy(() -> hazelcastInstanceFactory.newHazelcastInstance(wrongConfig))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("External data store 'non-existing-class' misconfigured: "
                        + "class 'com.example.NonExistingClass' not found");
    }
}


