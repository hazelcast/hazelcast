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

import com.hazelcast.jet.sql.impl.connector.jdbc.h2.H2UpsertQueryBuilder;
import com.hazelcast.jet.sql.impl.connector.jdbc.mssql.MSSQLUpsertQueryBuilder;
import com.hazelcast.jet.sql.impl.connector.jdbc.mysql.MySQLUpsertQueryBuilder;
import com.hazelcast.jet.sql.impl.connector.jdbc.oracle.OracleUpsertQueryBuilder;
import com.hazelcast.jet.sql.impl.connector.jdbc.postgres.PostgresUpsertQueryBuilder;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.H2SqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

final class UpsertBuilder {

    private UpsertBuilder() {
    }

    static String getUpsertStatement(JdbcTable jdbcTable, SqlDialect dialect) {

        String query = null;
        if (dialect instanceof MysqlSqlDialect) {
            MySQLUpsertQueryBuilder builder = new MySQLUpsertQueryBuilder(jdbcTable, dialect);
            query = builder.query();

        } else if (dialect instanceof PostgresqlSqlDialect) {
            PostgresUpsertQueryBuilder builder = new PostgresUpsertQueryBuilder(jdbcTable, dialect);
            query = builder.query();

        } else if (dialect instanceof H2SqlDialect) {
            H2UpsertQueryBuilder builder = new H2UpsertQueryBuilder(jdbcTable, dialect);
            query = builder.query();
        } else if (dialect instanceof MssqlSqlDialect) {
            MSSQLUpsertQueryBuilder builder = new MSSQLUpsertQueryBuilder(jdbcTable, dialect);
            query = builder.query();
        } else if (dialect instanceof OracleSqlDialect) {
            OracleUpsertQueryBuilder builder = new OracleUpsertQueryBuilder(jdbcTable, dialect);
            query = builder.query();
        }
        return query;
    }
}
