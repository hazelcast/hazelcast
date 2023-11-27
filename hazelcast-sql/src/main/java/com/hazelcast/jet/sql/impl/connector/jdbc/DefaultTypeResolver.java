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

import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Locale;

public class DefaultTypeResolver {

    private DefaultTypeResolver() {
    }

    /**
     * Convert the column type received from database to QueryDataType. QueryDataType represents the data types that
     * can be used in Hazelcast's distributed queries
     * TODO these contain dialect specific (non-standard) types, they should probably be moved to corresponding
     * dialects
     */
    @SuppressWarnings("ReturnCount")
    public static QueryDataType resolveType(String columnTypeName, int precision, int scale) {
        switch (columnTypeName.toUpperCase(Locale.ROOT)) {
            case "BOOLEAN":
            case "BOOL":
            case "BIT":
                return QueryDataType.BOOLEAN;

            case "VARCHAR":
            case "CHARACTER VARYING":
            case "TEXT":
            case "VARCHAR2":
                return QueryDataType.VARCHAR;

            case "TINYINT":
                return QueryDataType.TINYINT;

            case "SMALLINT":
            case "INT2":
                return QueryDataType.SMALLINT;

            case "INT":
            case "INT4":
            case "INTEGER":
                return QueryDataType.INT;

            case "INT8":
            case "BIGINT":
                return QueryDataType.BIGINT;

            case "DECIMAL":
            case "NUMERIC":
                return QueryDataType.DECIMAL;

            case "REAL":
            case "FLOAT":
            case "FLOAT4":
                return QueryDataType.REAL;

            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "FLOAT8":
                return QueryDataType.DOUBLE;

            case "DATE":
                return QueryDataType.DATE;

            case "TIME":
                return QueryDataType.TIME;

            case "TIMESTAMP":
            case "DATETIME":
            case "TIMESTAMP(6)":
                return QueryDataType.TIMESTAMP;

            case "TIMESTAMP WITH TIME ZONE":
            case "DATETIMEOFFSET":
            case "TIMESTAMP(6) WITH TIME ZONE":
                return QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;

            case "NULL":
                return QueryDataType.NULL;

            default:
                throw new IllegalArgumentException("Unsupported column type: " + columnTypeName);
        }
    }
}
