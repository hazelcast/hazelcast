/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl;

import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.type.QueryDataType;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;

/**
 * Common SQL engine utility methods used by both "core" and "sql" modules.
 */
public final class QueryUtils {

    public static final String WORKER_TYPE_OPERATION = "operation-worker";
    public static final String WORKER_TYPE_FRAGMENT = "fragment-worker";

    private static final String EXPLAIN = "explain ";

    private QueryUtils() {
        // No-op.
    }

    public static String workerName(String instanceName, String workerType, long index) {
        return instanceName + "-" + workerType + "-worker-" + index;
    }

    /**
     * Extract child path from the complex key-based path. E.g. "__key.field" => "field".
     *
     * @param path Original path.
     * @return Path without the key attribute or {@code null} if not a key.
     */
    public static String extractKeyPath(String path) {
        String prefix = KEY_ATTRIBUTE_NAME.value() + ".";

        return path.startsWith(prefix) ? path.substring(prefix.length()) : null;
    }

    public static boolean isExplain(String sql) {
        assert sql != null;

        return sql.toLowerCase().startsWith(EXPLAIN);
    }

    public static String unwrapExplain(String sql) {
        assert isExplain(sql);

        return sql.substring(EXPLAIN.length()).trim();
    }

    /**
     * Convert internal column type to public type.
     *
     * @param columnType Internal type.
     * @return Public type.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public static SqlColumnMetadata getColumnMetadata(QueryDataType columnType) {
        SqlColumnType type;

        switch (columnType.getTypeFamily()) {
            case VARCHAR:
                type = SqlColumnType.VARCHAR;

                break;

            case BIT:
                type = SqlColumnType.BIT;

                break;

            case TINYINT:
                type = SqlColumnType.TINYINT;

                break;

            case SMALLINT:
                type = SqlColumnType.SMALLINT;

                break;

            case INT:
                type = SqlColumnType.INT;

                break;

            case BIGINT:
                type = SqlColumnType.BIGINT;

                break;

            case DECIMAL:
                type = SqlColumnType.DECIMAL;

                break;

            case REAL:
                type = SqlColumnType.REAL;

                break;

            case DOUBLE:
                type = SqlColumnType.DOUBLE;

                break;

            case TIME:
                type = SqlColumnType.TIME;

                break;

            case DATE:
                type = SqlColumnType.DATE;

                break;

            case TIMESTAMP:
                type = SqlColumnType.TIMESTAMP;

                break;

            case TIMESTAMP_WITH_TIME_ZONE:
                type = SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;

                break;

            default:
                assert columnType == QueryDataType.OBJECT;

                type = SqlColumnType.OBJECT;
        }

        return new SqlColumnMetadata(type);
    }
}
