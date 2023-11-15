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
import com.hazelcast.test.jdbc.TestDatabaseProvider;

import javax.sql.CommonDataSource;

public abstract class DatabaseProviderTestSupport extends SimpleTestInClusterSupport {

    private static TestDatabaseProvider databaseProvider;

    public static TestDatabaseProvider getDatabaseProvider() {
        return databaseProvider;
    }

    public static void setDatabaseProvider(TestDatabaseProvider dbProvider) {
        databaseProvider = dbProvider;
        databaseProvider.createDatabase("test");
    }

    protected static String getJdbcUrl() {
        return databaseProvider.getJdbcUrl();
    }

    protected static String getUsername() {
        return databaseProvider.user();
    }

    protected static String getPassword() {
        return databaseProvider.password();
    }

    protected static String getDatabaseName() {
        return databaseProvider.getDatabaseName();
    }

    protected static CommonDataSource createDataSource(boolean xa) {
        return databaseProvider.createDataSource(xa);
    }

    public static void shutdownDatabaseProvider() {
        if (databaseProvider != null) {
            databaseProvider.shutdown();
            databaseProvider = null;
        }
    }
}
