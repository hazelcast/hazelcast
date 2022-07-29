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
import com.hazelcast.spi.annotation.Beta;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

/**
 * Creates a JDBC data store as a {@link DataSource}
 *
 * @since 5.2
 */
@Beta
public class JdbcDataStoreFactory implements ExternalDataStoreFactory<DataSource> {
    private static final String JDBC_URL = "jdbc.url";
    private static final String JDBC_USERNAME = "jdbc.username";
    private static final String JDBC_PASSWORD = "jdbc.password";
    private ExternalDataStoreConfig config;
    private DataSource shareDataSource;

    public void init(ExternalDataStoreConfig config) {
        this.config = config;
        if (config.isShared()) {
            shareDataSource = createDataSource();
        }
    }

    @Override
    public DataSource getDataStore() {
        return config.isShared() ? shareDataSource : createDataSource();
    }

    private DataSource createDataSource() {
        String jdbcUrl = config.getProperty(JDBC_URL);
        HikariConfig dataSourceConfig = new HikariConfig();
        dataSourceConfig.setJdbcUrl(jdbcUrl);
        dataSourceConfig.setUsername(config.getProperty(JDBC_USERNAME));
        dataSourceConfig.setPassword(config.getProperty(JDBC_PASSWORD));
        return new HikariDataSource(dataSourceConfig);
    }
}
