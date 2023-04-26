/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;

public final class ConnectionUtils {

    private ConnectionUtils() {
    }

    public static boolean isMySQL(DatabaseMetaData databaseMetaData) throws SQLException {
        return getProductName(databaseMetaData).equals("MYSQL");
    }

    private static String getProductName(DatabaseMetaData databaseMetaData) throws SQLException {
        return databaseMetaData.getDatabaseProductName().toUpperCase(Locale.ROOT).trim();
    }
}
