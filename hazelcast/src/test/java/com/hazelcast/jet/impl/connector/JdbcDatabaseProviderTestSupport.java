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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.test.jdbc.JdbcDatabaseProvider;

import javax.sql.CommonDataSource;

/**
 * Test support class that contains TestDatabaseProvider
 */
public abstract class JdbcDatabaseProviderTestSupport extends SimpleTestInClusterSupport {

    private static JdbcDatabaseProvider jdbcDatabaseProvider;

    public static JdbcDatabaseProvider getJdbcDatabaseProvider() {
        return jdbcDatabaseProvider;
    }

    public static void setJdbcDatabaseProvider(JdbcDatabaseProvider dbProvider) {
        jdbcDatabaseProvider = dbProvider;
        jdbcDatabaseProvider.createDatabase("test");
    }

    protected static String getJdbcUrl() {
        return jdbcDatabaseProvider.url();
    }

    protected static String getUsername() {
        return jdbcDatabaseProvider.user();
    }

    protected static String getPassword() {
        return jdbcDatabaseProvider.password();
    }

    protected static String getDatabaseName() {
        return jdbcDatabaseProvider.getDatabaseName();
    }

    protected static CommonDataSource createDataSource(boolean xa) {
        return jdbcDatabaseProvider.createDataSource(xa);
    }

    public static void shutdownDatabaseProvider() {
        if (jdbcDatabaseProvider != null) {
            jdbcDatabaseProvider.shutdown();
            jdbcDatabaseProvider = null;
        }
    }
}
