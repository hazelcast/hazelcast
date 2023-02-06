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

import com.hazelcast.config.Config;
import com.hazelcast.config.ExternalDataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.datalink.ExternalDataLinkFactory;
import com.hazelcast.datalink.ExternalDataLinkService;
import com.hazelcast.datalink.JdbcDataLinkFactory;
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

import static com.hazelcast.datalink.impl.HikariTestUtil.assertDataSourceClosed;
import static com.hazelcast.datalink.impl.HikariTestUtil.assertEventuallyNoHikariThreads;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExternalDataLinkServiceImplTest extends HazelcastTestSupport {
    private static final String TEST_CONFIG_NAME = ExternalDataLinkServiceImplTest.class.getSimpleName();

    private final Config config = smallInstanceConfig();
    private final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private ExternalDataLinkService externalDataLinkService;

    @Before
    public void configure() {
        ExternalDataLinkConfig externalDataLinkConfig = new ExternalDataLinkConfig()
                .setName(TEST_CONFIG_NAME)
                .setClassName("com.hazelcast.datalink.JdbcDataLinkFactory")
                .setProperty("jdbcUrl", "jdbc:h2:mem:" + ExternalDataLinkServiceImplTest.class.getSimpleName());
        config.addExternalDataLinkConfig(externalDataLinkConfig);
        externalDataLinkService = getExternalDataLinkService();
    }

    @After
    public void tearDown() throws Exception {
        hazelcastInstanceFactory.shutdownAll();
        assertEventuallyNoHikariThreads(TEST_CONFIG_NAME);
    }

    @Test
    public void test_connection_should_return_TRUE_when_dataLink_exists() throws Exception {
        ExternalDataLinkConfig externalDSConfig = config.getExternalDataLinkConfig(TEST_CONFIG_NAME);

        assertThat(externalDataLinkService.testConnection(externalDSConfig)).isTrue();
    }

    @Test
    public void test_connection_should_throw_when_no_db_is_present() {
        ExternalDataLinkConfig externalDSConfig = new ExternalDataLinkConfig()
                .setName(TEST_CONFIG_NAME)
                .setClassName("com.hazelcast.datalink.JdbcDataLinkFactory")
                .setProperty("jdbcUrl", "jdbc:h2:file:/random/path;IFEXISTS=TRUE");

        assertThatThrownBy(() -> externalDataLinkService.testConnection(externalDSConfig))
                .hasMessageContaining("Database \"/random/path\" not found");
    }

    @Test
    public void test_connection_should_throw_when_no_suitable_driver_is_present() {
        ExternalDataLinkConfig externalDSConfig = new ExternalDataLinkConfig()
                .setName(TEST_CONFIG_NAME)
                .setClassName("com.hazelcast.datalink.JdbcDataLinkFactory")
                .setProperty("jdbcUrl", "jdbc:mysql:localhost:3306/random_db");


        assertThatThrownBy(() -> externalDataLinkService.testConnection(externalDSConfig))
                .isExactlyInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(SQLException.class)
                .hasRootCauseMessage("No suitable driver");
    }

    @Test
    public void should_return_working_dataLink() throws Exception {
        ExternalDataLinkFactory<?> dataLinkFactory = externalDataLinkService.getExternalDataLinkFactory(TEST_CONFIG_NAME);
        assertInstanceOf(JdbcDataLinkFactory.class, dataLinkFactory);

        DataSource dataSource = ((JdbcDataLinkFactory) dataLinkFactory).getDataLink();

        String actualName;
        try (ResultSet resultSet = executeQuery(dataSource, "select 'some-name' as name")) {
            resultSet.next();
            actualName = resultSet.getString(1);
        }

        assertThat(actualName).isEqualTo("some-name");
    }

    @Test
    public void should_fail_when_non_existing_dataLink() {
        assertThatThrownBy(() -> externalDataLinkService.getExternalDataLinkFactory("non-existing-data-link"))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("External data link factory 'non-existing-data-link' not found");
    }

    @Test
    public void should_close_factories() {
        ExternalDataLinkFactory<?> dataLinkFactory = externalDataLinkService.getExternalDataLinkFactory(TEST_CONFIG_NAME);

        DataSource dataSource = ((JdbcDataLinkFactory) dataLinkFactory).getDataLink();
        externalDataLinkService.close();

        assertDataSourceClosed(dataSource, TEST_CONFIG_NAME);
    }

    private ExternalDataLinkService getExternalDataLinkService() {
        HazelcastInstance instance = hazelcastInstanceFactory.newHazelcastInstance(config);
        return Util.getNodeEngine(instance).getExternalDataLinkService();
    }

    private ResultSet executeQuery(DataSource dataSource, String sql) throws SQLException {
        return dataSource.getConnection().prepareStatement(sql).executeQuery();
    }

    @Test
    public void should_fail_when_datastore_class_DOES_NOT_implements_ExternalDataLinkFactory() {
        Config wrongConfig = smallInstanceConfig();

        ExternalDataLinkConfig externalDataLinkConfig = new ExternalDataLinkConfig()
                .setName("wrong-class-name")
                .setClassName("java.lang.Object");
        wrongConfig.addExternalDataLinkConfig(externalDataLinkConfig);

        assertThatThrownBy(() -> hazelcastInstanceFactory.newHazelcastInstance(wrongConfig))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("External data link 'wrong-class-name' misconfigured: 'java.lang.Object'"
                        + " must implement 'com.hazelcast.datalink.ExternalDataLinkFactory'");
    }

    @Test
    public void should_fail_when_datalink_class_NON_existing() {
        Config wrongConfig = smallInstanceConfig();

        ExternalDataLinkConfig externalDataLinkConfig = new ExternalDataLinkConfig()
                .setName("non-existing-class")
                .setClassName("com.example.NonExistingClass");
        wrongConfig.addExternalDataLinkConfig(externalDataLinkConfig);

        assertThatThrownBy(() -> hazelcastInstanceFactory.newHazelcastInstance(wrongConfig))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("External data link 'non-existing-class' misconfigured: "
                        + "class 'com.example.NonExistingClass' not found");
    }
}


