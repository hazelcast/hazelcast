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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Hazelcast Oracle Dialect for setting cast spec of varchar
 */
public class HazelcastOracleDialect extends OracleSqlDialect {
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
    public static String isNumberTypeCheck(Object metaData, Object type, Object column) throws SQLException {
        int col = (int) column;
        if (type.equals("NUMBER")) {
            int precision = ((ResultSetMetaData) metaData).getPrecision(col + 1);
            int scale = ((ResultSetMetaData) metaData).getScale(col + 1);

            if (scale == INT_SCALE) {
                if (precision <= SMALLINT_PRECISION) {
                    return "SMALLINT";
                } else if (precision < INT_PRECISION) {
                    return "INT";
                } else if (precision <= BIGINT_PRECISION) {
                    return "BIGINT";
                } else {
                    return "DECIMAL";
                }
            } else {
                if ((scale + precision) <= REAL_PRECISION_SCALE) {
                    return "REAL";
                } else if ((scale + precision) <= DOUBLE_PRECISION_SCALE) {
                    return "DOUBLE PRECISION";
                }
            }

        }
        /*
            If none of the conditions above are met, then the default conversion for
            NUMBER(p,s) where s != 0 will be DECIMAL(p,s).
        */
        return (String) type;
    }
}
