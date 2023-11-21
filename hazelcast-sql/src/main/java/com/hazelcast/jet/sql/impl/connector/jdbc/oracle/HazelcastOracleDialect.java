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

package com.hazelcast.jet.sql.impl.connector.jdbc.oracle;

import com.hazelcast.jet.sql.impl.connector.jdbc.DefaultTypeResolver;
import com.hazelcast.jet.sql.impl.connector.jdbc.TypeResolver;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Locale;

/**
 * Hazelcast Oracle Dialect for setting cast spec of varchar
 */
public class HazelcastOracleDialect extends OracleSqlDialect implements TypeResolver {

    private static final int DOUBLE_PRECISION_SCALE = 15;
    private static final int INT_PRECISION = 10;
    private static final int SMALLINT_PRECISION = 4;
    private static final int BIGINT_PRECISION = 18;
    private static final int INT_SCALE = 0;
    private static final int REAL_PRECISION_SCALE = 7;

    /**
     * Creates a HazelcastOracleDialect.
     */
    public HazelcastOracleDialect(Context context) {
        super(context);
    }

    @Override
    public @Nullable SqlNode getCastSpec(RelDataType type) {
        if (type.getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
            return new SqlDataTypeSpec(
                    new SqlAlienSystemTypeNameSpec("VARCHAR(128)", type.getSqlTypeName(), SqlParserPos.ZERO),
                    SqlParserPos.ZERO);
        } else {
            return super.getCastSpec(type);
        }
    }

    @Override
    public QueryDataType resolveType(String columnTypeName, int precision, int scale) {
        switch (columnTypeName.toUpperCase(Locale.ROOT)) {
            case "NUMBER":
                return resolveNumber(precision, scale);

            default:
                return DefaultTypeResolver.resolveType(columnTypeName, precision, scale);
        }
    }

    private static QueryDataType resolveNumber(int precision, int scale) {
        if (scale == INT_SCALE) {
            if (precision <= SMALLINT_PRECISION) {
                return QueryDataType.SMALLINT;
            } else if (precision < INT_PRECISION) {
                return QueryDataType.INT;
            } else if (precision <= BIGINT_PRECISION) {
                return QueryDataType.BIGINT;
            }
        } else {
            if ((scale + precision) <= REAL_PRECISION_SCALE) {
                return QueryDataType.REAL;
            } else if ((scale + precision) <= DOUBLE_PRECISION_SCALE) {
                return QueryDataType.DOUBLE;
            }
        }
        return QueryDataType.DECIMAL;
    }
}
