/*
 * Copyright 2025 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc.postgres;

import com.hazelcast.jet.sql.impl.connector.jdbc.DefaultTypeResolver;
import com.hazelcast.jet.sql.impl.connector.jdbc.TypeResolver;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

import java.util.Locale;

/**
 * Hazelcast Postgres Dialect for setting cast spec of varchar
 */
public class HazelcastPostgresDialect extends PostgresqlSqlDialect implements TypeResolver {
    /**
     * Creates a PostgresqlSqlDialect.
     */
    public HazelcastPostgresDialect(Context context) {
        super(context);
    }

    @Override
    @SuppressWarnings("ReturnCount")
    public QueryDataType resolveType(String columnTypeName, int precision, int scale) {
        switch (columnTypeName.toUpperCase(Locale.ROOT)) {
            case "BOOL":
            case "BIT":
                return QueryDataType.BOOLEAN;

            case "BPCHAR":
            case "VARCHAR":
            case "TEXT":
                return QueryDataType.VARCHAR;

            case "INT2":
            case "SMALLSERIAL":
                return QueryDataType.SMALLINT;

            case "INT4":
            case "SERIAL":
                return QueryDataType.INT;

            case "INT8":
            case "BIGSERIAL":
                return QueryDataType.BIGINT;

            case "NUMERIC":
                return QueryDataType.DECIMAL;

            case "FLOAT4":
                return QueryDataType.REAL;

            case "FLOAT8":
                return QueryDataType.DOUBLE;

            case "DATE":
                return QueryDataType.DATE;

            case "TIME":
                return QueryDataType.TIME;

            case "TIMESTAMP":
                return QueryDataType.TIMESTAMP;

            case "TIMESTAMPTZ":
                return QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;

            default:
                return DefaultTypeResolver.resolveType(columnTypeName, precision, scale);
        }
    }
}
