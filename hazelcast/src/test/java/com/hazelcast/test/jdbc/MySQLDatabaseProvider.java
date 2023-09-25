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

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;

import java.util.Arrays;
import java.util.Map;

import static java.util.stream.Collectors.joining;

public class MySQLDatabaseProvider implements TestDatabaseProvider {

    public static final String TEST_MYSQL_VERSION = System.getProperty("test.mysql.version", "8.0.32");
    private static final int LOGIN_TIMEOUT = 120;

    private MySQLContainer<?> container;
    private Network network = Network.newNetwork();

    @Override
    public String createDatabase(String dbName) {
        //noinspection resource
        container = new MySQLContainer<>("mysql:" + TEST_MYSQL_VERSION)
                .withNetwork(network)
                .withNetworkAliases("mysql")
                .withDatabaseName(dbName)
                .withUsername("root")
                .withUrlParam("user", "root")
                .withUrlParam("password", "test")
                .withTmpFs(Map.of(
                        "/var/lib/mysql/", "rw",
                        "/tmp/", "rw"
                ));
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
    public String user() {
        return "root";
    }

    @Override
    public String password() {
        return "test";
    }

    public MySQLContainer<?> container() {
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
    public String quote(String[] parts) {
        return Arrays.stream(parts)
                     .map(part -> '`' + part.replaceAll("`", "``") + '`')
                     .collect(joining("."));

    }
}
