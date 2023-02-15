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
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class SupportedDatabases {

    private static final ILogger LOGGER = Logger.getLogger(SupportedDatabases.class);

    private static final Set<String> DATABASE_NAMES = ConcurrentHashMap.newKeySet();

    static {
        // Add supported database names in upper case
        DATABASE_NAMES.add("MYSQL");
        DATABASE_NAMES.add("POSTGRESQL");
        DATABASE_NAMES.add("H2");
        //DATABASE_NAMES.add("MICROSOFT SQL SERVER");
    }

    private SupportedDatabases() {
    }

    static void logOnceIfDatabaseNotSupported(DatabaseMetaData databaseMetaData) throws SQLException {
        // Get product name from the JDBC driver
        String databaseProductName = databaseMetaData.getDatabaseProductName();
        logOnceByProductName(databaseProductName);
    }

    static boolean logOnceByProductName(String databaseProductName) {
        // Make the DB name upper case
        String uppercaseProductName = StringUtil.upperCaseInternal(databaseProductName);

        boolean newDatabaseName = DATABASE_NAMES.add(uppercaseProductName);
        if (newDatabaseName) {
            // If this Database name is new, log a message
            LOGGER.warning("Database " + uppercaseProductName + " is not officially supported");
        }
        return newDatabaseName;
    }

    static boolean isDialectSupported(JdbcTable jdbcTable) {
        SqlDialect dialect = jdbcTable.sqlDialect();
        return dialect instanceof MysqlSqlDialect ||
               dialect instanceof PostgresqlSqlDialect ||
               dialect instanceof H2SqlDialect;
               //dialect instanceof MssqlSqlDialect
    }
}
