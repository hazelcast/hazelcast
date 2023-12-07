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
package com.hazelcast.test.jdbc;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;

import java.util.Properties;

public abstract class JdbcDatabaseProvider<C extends JdbcDatabaseContainer<?>> implements TestDatabaseProvider {

    private static final int LOGIN_TIMEOUT = 120;

    protected C container;
    protected final Network network = Network.newNetwork();

    abstract C createContainer(String dbName);

    @Override
    public String createDatabase(String dbName) {
        container = createContainer(dbName);
        container.start();
        String jdbcUrl = container.getJdbcUrl();
        waitForDb(jdbcUrl, LOGIN_TIMEOUT);
        return jdbcUrl;
    }

    @Override
    public String noAuthJdbcUrl() {
        return container.getJdbcUrl()
                        .replaceAll("&?user=root", "")
                        .replaceAll("&?password=test", "");
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

    public JdbcDatabaseContainer<?> container() {
        return container;
    }

    public Network getNetwork() {
        return network;
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
