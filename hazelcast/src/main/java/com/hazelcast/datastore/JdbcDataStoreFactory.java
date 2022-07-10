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
import com.hazelcast.jet.impl.connector.DataSourceFromConnectionSupplier;

import javax.sql.DataSource;

public class JdbcDataStoreFactory implements ExternalDataStoreFactory<DataSource> {
    private static final String JDBC_URL = "jdbc.url";
    private static final String JDBC_USERNAME = "jdbc.username";
    private static final String JDBC_PASSWORD = "jdbc.password";
    private ExternalDataStoreConfig config;

    public void init(ExternalDataStoreConfig config) {
        this.config = config;
    }

    @Override
    public DataSource createDataStore() {
        String jdbcUrl = config.getProperty(JDBC_URL);
        if (config.isShared()) {
            throw new UnsupportedOperationException("shared flag is not yet supported");
        }
        if (config.getProperties().containsKey(JDBC_USERNAME) || config.getProperties().containsKey(JDBC_PASSWORD)) {
            String username = config.getProperty(JDBC_USERNAME);
            String password = config.getProperty(JDBC_PASSWORD);
            return new DataSourceFromConnectionSupplier(jdbcUrl, username, password);
        }
        //TODO Use HikariDataSource for pooling or similar
        return new DataSourceFromConnectionSupplier(jdbcUrl);
    }
}
