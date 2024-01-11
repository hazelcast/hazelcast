/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import org.testcontainers.containers.MariaDBContainer;

import javax.sql.CommonDataSource;
import java.util.Arrays;

import static java.util.stream.Collectors.joining;

public class MariaDBDatabaseProvider extends JdbcDatabaseProvider<MariaDBContainer<?>> {

    public static final String TEST_MARIADB_VERSION = System.getProperty("test.mariadb.version", "10.3");


    @Override
    public CommonDataSource createDataSource(boolean xa) {
        throw new RuntimeException("Not supported");
    }

    @SuppressWarnings("resource")
    @Override
    MariaDBContainer<?> createContainer(String dbName) {
        return new MariaDBContainer<>("mariadb:" + TEST_MARIADB_VERSION)
                .withDatabaseName(dbName)
                .withUsername(user())
                .withUrlParam("user", user())
                .withUrlParam("password", password());
    }

    @Override
    public String user() {
        return "user";
    }


    @Override
    public String quote(String[] parts) {
        return Arrays.stream(parts)
                     .map(part -> '`' + part.replaceAll("`", "``") + '`')
                     .collect(joining("."));

    }
}
