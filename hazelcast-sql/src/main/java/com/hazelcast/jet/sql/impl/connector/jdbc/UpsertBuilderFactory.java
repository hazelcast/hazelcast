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

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.H2SqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

final class UpsertBuilderFactory {

    private UpsertBuilderFactory() {

    }

    // Returns if upsert is supported for the given dialect
    static boolean isUpsertSupported(JdbcTable jdbcTable) {
        boolean result = false;
        SqlDialect dialect = jdbcTable.sqlDialect();
        if (dialect instanceof MysqlSqlDialect ||
            dialect instanceof PostgresqlSqlDialect ||
            dialect instanceof H2SqlDialect) {
            result = true;
        }
        return result;
    }

    static String getUpsertQuery(JdbcTable jdbcTable) {

        SqlDialect sqlDialect = jdbcTable.sqlDialect();

        String query = null;
        if (sqlDialect instanceof MysqlSqlDialect) {
            MySQLUpsertQueryBuilder builder = new MySQLUpsertQueryBuilder(jdbcTable, sqlDialect);
            query = builder.query();

        } else if (sqlDialect instanceof PostgresqlSqlDialect) {
            PostgreSQLUpsertQueryBuilder builder = new PostgreSQLUpsertQueryBuilder(jdbcTable, sqlDialect);
            query = builder.query();

        } else if (sqlDialect instanceof H2SqlDialect) {
            H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(jdbcTable, sqlDialect);
            query = builder.query();
        }
        return query;
    }
}
