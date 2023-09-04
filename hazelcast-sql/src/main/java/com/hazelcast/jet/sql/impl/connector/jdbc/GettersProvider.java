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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.sql.impl.connector.jdbc.mssql.HazelcastMSSQLDialect;

import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("checkstyle:ExecutableStatementCount")
final class GettersProvider {

    private static final Map<String, BiFunctionEx<ResultSet, Integer, Object>> DEFAULT_GETTERS = new HashMap<>();
    private static final Map<String, Map<String, BiFunctionEx<ResultSet, Integer, Object>>> GETTERS_BY_DATABASE
            = new HashMap<>();

    static {
        DEFAULT_GETTERS.put("BOOLEAN", ResultSet::getBoolean);
        DEFAULT_GETTERS.put("BOOL", ResultSet::getBoolean);

        DEFAULT_GETTERS.put("TINYINT", ResultSet::getByte);

        DEFAULT_GETTERS.put("SMALLINT", ResultSet::getShort);
        DEFAULT_GETTERS.put("INT2", ResultSet::getShort);

        DEFAULT_GETTERS.put("INT", ResultSet::getInt);
        DEFAULT_GETTERS.put("INT4", ResultSet::getInt);
        DEFAULT_GETTERS.put("INTEGER", ResultSet::getInt);

        DEFAULT_GETTERS.put("INT8", ResultSet::getLong);
        DEFAULT_GETTERS.put("BIGINT", ResultSet::getLong);

        DEFAULT_GETTERS.put("VARCHAR", ResultSet::getString);
        DEFAULT_GETTERS.put("CHARACTER VARYING", ResultSet::getString);
        DEFAULT_GETTERS.put("TEXT", ResultSet::getString);

        DEFAULT_GETTERS.put("REAL", ResultSet::getFloat);
        DEFAULT_GETTERS.put("FLOAT", ResultSet::getFloat);
        DEFAULT_GETTERS.put("FLOAT4", ResultSet::getFloat);

        DEFAULT_GETTERS.put("DOUBLE", ResultSet::getDouble);
        DEFAULT_GETTERS.put("DOUBLE PRECISION", ResultSet::getDouble);

        DEFAULT_GETTERS.put("DECIMAL", ResultSet::getBigDecimal);
        DEFAULT_GETTERS.put("NUMERIC", ResultSet::getBigDecimal);

        DEFAULT_GETTERS.put("DATE", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDate.class));
        DEFAULT_GETTERS.put("TIME", (rs, columnIndex) -> rs.getObject(columnIndex, LocalTime.class));
        DEFAULT_GETTERS.put("TIMESTAMP", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
        DEFAULT_GETTERS.put("TIMESTAMP_WITH_TIMEZONE",
                (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));
        DEFAULT_GETTERS.put("TIMESTAMP WITH TIME ZONE", (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));

        // Override some getters for MS SQL
        Map<String, BiFunctionEx<ResultSet, Integer, Object>> msSql = new HashMap<>(DEFAULT_GETTERS);
        msSql.put("FLOAT", ResultSet::getDouble);
        msSql.put("DATETIME", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
        msSql.put("DATETIMEOFFSET", (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));
        GETTERS_BY_DATABASE.put(HazelcastMSSQLDialect.class.getSimpleName(), msSql);

    }

    private GettersProvider() {
    }

    public static Map<String, BiFunctionEx<ResultSet, Integer, Object>> getGetters(String dialect) {
        return GETTERS_BY_DATABASE.getOrDefault(dialect, DEFAULT_GETTERS);
    }
}
