/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.test.jdbc;

import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.sql.CommonDataSource;
import java.util.Properties;

public abstract class JdbcDatabaseProvider<C extends JdbcDatabaseContainer<?>> implements TestDatabaseProvider {

    private static final int LOGIN_TIMEOUT = 120;

    protected C container;

    public abstract CommonDataSource createDataSource(boolean xa);

    abstract C createContainer(String dbName);

    @Override
    public String createDatabase(String dbName) {
        container = createContainer(dbName);
        container.start();
        String jdbcUrl = url();
        waitForDb(jdbcUrl, LOGIN_TIMEOUT);
        return jdbcUrl;
    }

    @Override
    public String noAuthJdbcUrl() {
        return container.getJdbcUrl()
                        .replaceAll("&?user=root", "")
                        .replaceAll("&?password=test", "");
    }

    public String getDatabaseName() {
        return container.getDatabaseName();
    }

    @Override
    public String url() {
        return container.getJdbcUrl();
    }

    @Override
    public String user() {
        return "root";
    }

    @Override
    public String password() {
        return "test";
    }

    public C container() {
        return container;
    }

    @Override
    public void shutdown() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }

    @Override
    public Properties properties() {
        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", url());
        return properties;
    }

    @Override
    public TestDatabaseRecordProvider recordProvider() {
        return new JdbcObjectProvider(this);
    }
}
