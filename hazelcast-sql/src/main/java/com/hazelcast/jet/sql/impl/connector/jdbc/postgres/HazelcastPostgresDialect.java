/*
 * Copyright 2024 Hazelcast Inc.
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
    public QueryDataType resolveType(String columnTypeName, int precision, int scale) {
        if (columnTypeName.toUpperCase(Locale.ROOT).equals("BPCHAR")) {
            return QueryDataType.VARCHAR;
        }
        return DefaultTypeResolver.resolveType(columnTypeName, precision, scale);
    }

}
