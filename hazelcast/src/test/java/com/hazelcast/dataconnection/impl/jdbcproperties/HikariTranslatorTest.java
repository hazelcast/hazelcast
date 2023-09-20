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

package com.hazelcast.dataconnection.impl.jdbcproperties;

import com.zaxxer.hikari.HikariConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class HikariTranslatorTest {

    HikariTranslator hikariTranslator;

    @Before
    public void setUp() {
        hikariTranslator = new HikariTranslator(new AtomicInteger(), "foo");
    }

    @Test
    public void testTranslatableProperties() {
        Properties hzProperties = new Properties();
        String jdbcUrl = "jdbcUrl";
        String connectionTimeout = "5000";
        String idleTimeout = "6000";
        String keepAliveTime = "7000";
        String maxLifetime = "8000";
        String minimumIdle = "8500";
        String maximumPoolSize = "10";

        hzProperties.put(DataConnectionProperties.JDBC_URL, jdbcUrl);
        hzProperties.put(DataConnectionProperties.CONNECTION_TIMEOUT, connectionTimeout);
        hzProperties.put(DataConnectionProperties.IDLE_TIMEOUT, idleTimeout);
        hzProperties.put(DataConnectionProperties.KEEP_ALIVE_TIME, keepAliveTime);
        hzProperties.put(DataConnectionProperties.MAX_LIFETIME, maxLifetime);
        hzProperties.put(DataConnectionProperties.MINIMUM_IDLE, minimumIdle);
        hzProperties.put(DataConnectionProperties.MAXIMUM_POOL_SIZE, maximumPoolSize);

        Properties hikariProperties = hikariTranslator.translate(hzProperties);
        HikariConfig hikariConfig = new HikariConfig(hikariProperties);

        assertThat(hikariConfig.getJdbcUrl()).isEqualTo(jdbcUrl);
        assertThat(hikariConfig.getConnectionTimeout()).isEqualTo(Long.parseLong(connectionTimeout));
        assertThat(hikariConfig.getIdleTimeout()).isEqualTo(Long.parseLong(idleTimeout));
        assertThat(hikariConfig.getKeepaliveTime()).isEqualTo(Long.parseLong(keepAliveTime));
        assertThat(hikariConfig.getMaxLifetime()).isEqualTo(Long.parseLong(maxLifetime));
        assertThat(hikariConfig.getMinimumIdle()).isEqualTo(Long.parseLong(minimumIdle));
        assertThat(hikariConfig.getMaximumPoolSize()).isEqualTo(Long.parseLong(maximumPoolSize));
    }

    @Test
    public void testHikariSpecificProperty() {
        Properties hzProperties = new Properties();
        String connectionInitSql = "foo";

        hzProperties.put("hikari.connectionInitSql", connectionInitSql);
        hzProperties.put("hikariabc", "value");

        Properties hikariProperties = hikariTranslator.translate(hzProperties);
        assertThat(hikariProperties).containsEntry("dataSource.hikariabc", "value");
        HikariConfig hikariConfig = new HikariConfig(hikariProperties);

        assertThat(hikariConfig.getConnectionInitSql()).isEqualTo(connectionInitSql);
    }

    @Test
    public void testPoolNameProperty() {
        Properties hzProperties = new Properties();

        Properties hikariProperties = hikariTranslator.translate(hzProperties);
        HikariConfig hikariConfig = new HikariConfig(hikariProperties);

        String poolName = hikariConfig.getPoolName();
        assertThat(poolName).isEqualTo("HikariPool-0-foo");
    }

    @Test
    public void testUnknownProperty() {
        // Unknown Hikari property is considered as DataSource property
        String unknownProperty = "unknownProperty";
        Properties hzProperties = new Properties();
        hzProperties.put(unknownProperty, "value");

        Properties hikariProperties = hikariTranslator.translate(hzProperties);
        assertThat(hikariProperties).containsEntry("dataSource.unknownProperty", "value");
        HikariConfig hikariConfig = new HikariConfig(hikariProperties);

        // Get DataSource for verification
        Properties dataSourceProperties = hikariConfig.getDataSourceProperties();
        assertThat(dataSourceProperties.getProperty(unknownProperty)).isEqualTo("value");
    }

    @Test
    public void testUserAndPassword() {
        // Unknown Hikari property is considered as DataSource property
        Properties hzProperties = new Properties();
        hzProperties.put("user", "testUser");
        hzProperties.put("password", "testPassword");

        Properties hikariProperties = hikariTranslator.translate(hzProperties);
        assertThat(hikariProperties).containsEntry("dataSource.user", "testUser");
        assertThat(hikariProperties).containsEntry("dataSource.password", "testPassword");
    }

}
