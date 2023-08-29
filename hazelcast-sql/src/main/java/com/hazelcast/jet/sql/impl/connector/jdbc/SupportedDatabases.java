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

import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.H2SqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;


import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class SupportedDatabases {

    private static final ILogger LOGGER = Logger.getLogger(SupportedDatabases.class);

    private static final Set<String> SUPPORTED_DATABASE_NAMES = new HashSet<>();
    private static final Set<String> DETECTED_DATABASE_NAMES = ConcurrentHashMap.newKeySet();

    static {
        // Add supported database names in upper case
        SUPPORTED_DATABASE_NAMES.add("MYSQL");
        SUPPORTED_DATABASE_NAMES.add("POSTGRESQL");
        SUPPORTED_DATABASE_NAMES.add("H2");
        SUPPORTED_DATABASE_NAMES.add("MICROSOFT SQL SERVER");
        SUPPORTED_DATABASE_NAMES.add("ORACLE");
    }

    private SupportedDatabases() {
    }

    static void logOnceIfDatabaseNotSupported(DatabaseMetaData databaseMetaData) throws SQLException {
        String uppercaseProductName = getProductName(databaseMetaData);

        boolean newDatabaseName = isNewDatabase(uppercaseProductName);
        if (newDatabaseName) {
            LOGGER.warning("Database " + uppercaseProductName + " is not supported, it may or may not work. "
                    + "If you come across any issues please report them on Github.");
        }
    }

    private static String getProductName(DatabaseMetaData databaseMetaData) throws SQLException {
        String productName = databaseMetaData.getDatabaseProductName();
        return StringUtil.upperCaseInternal(productName);
    }

    static boolean isNewDatabase(String uppercaseProductName) {
        if (SUPPORTED_DATABASE_NAMES.contains(uppercaseProductName)) {
            return false;
        }
        return DETECTED_DATABASE_NAMES.add(uppercaseProductName);
    }

    static boolean isDialectSupported(SqlDialect dialect) {
        return dialect instanceof H2SqlDialect ||
               dialect instanceof MssqlSqlDialect ||
               dialect instanceof MysqlSqlDialect ||
               dialect instanceof OracleSqlDialect ||
               dialect instanceof PostgresqlSqlDialect;
    }
}
