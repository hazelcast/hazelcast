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

package com.hazelcast.datastore;

import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.datastore.impl.CloseableDataSource;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.spi.annotation.Beta;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates a JDBC data store as a {@link DataSource}.
 * <p>
 * Implementation is based on {@link HikariDataSource}. {@link ExternalDataStoreConfig#getProperties()} are passed directly
 * to {@link HikariConfig}. For available options see
 * <a href="https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby">HikariCP configuration</a>
 * </p>
 *
 * @since 5.2
 */
@Beta
public class JdbcDataStoreFactory implements ExternalDataStoreFactory<DataSource> {

    private static final int JDBC_TEST_CONNECTION_TIMEOUT_SECONDS = 5;
    private static final AtomicInteger DATA_SOURCE_COUNTER = new AtomicInteger();

    protected HikariDataSource sharedDataSource;
    protected CloseableDataSource sharedCloseableDataSource;
    protected ExternalDataStoreConfig config;

    @Override
    public void init(ExternalDataStoreConfig config) {
        this.config = config;
        if (config.isShared()) {
            sharedDataSource = doCreateDataSource();
            sharedCloseableDataSource = CloseableDataSource.nonClosing(sharedDataSource);
        }
    }

    @Override
    public DataSource getDataStore() {
        return config.isShared() ? sharedCloseableDataSource : CloseableDataSource.closing(doCreateDataSource());
    }

    @Override
    public boolean testConnection() throws Exception {
        try (CloseableDataSource dataStore = ((CloseableDataSource) getDataStore());
             Connection connection = dataStore.getConnection()) {
            return connection.isValid(JDBC_TEST_CONNECTION_TIMEOUT_SECONDS);
        }
    }

    protected HikariDataSource doCreateDataSource() {
        Properties properties = new Properties();
        properties.putAll(config.getProperties());
        if (!properties.containsKey("poolName")) {
            String suffix = StringUtil.isNullOrEmpty(config.getName()) ? "" : "-" + config.getName();
            properties.put("poolName", "HikariPool-" + DATA_SOURCE_COUNTER.getAndIncrement() + suffix);
        }
        HikariConfig dataSourceConfig = new HikariConfig(properties);
        return new HikariDataSource(dataSourceConfig);
    }

    @Override
    public void close() throws Exception {
        if (sharedDataSource != null) {
            sharedDataSource.close();
        }
    }
}
