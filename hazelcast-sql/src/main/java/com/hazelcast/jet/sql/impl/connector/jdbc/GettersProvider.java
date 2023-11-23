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
import com.hazelcast.sql.impl.type.QueryDataType;

import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.DATE;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataType.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.REAL;
import static com.hazelcast.sql.impl.type.QueryDataType.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataType.TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@SuppressWarnings("checkstyle:ExecutableStatementCount")
final class GettersProvider {

    public static final Map<QueryDataType, BiFunctionEx<ResultSet, Integer, ?>> GETTERS = new HashMap<>();

    static {
        GETTERS.put(BOOLEAN, ResultSet::getBoolean);
        GETTERS.put(TINYINT, ResultSet::getByte);
        GETTERS.put(SMALLINT, ResultSet::getShort);
        GETTERS.put(INT, ResultSet::getInt);
        GETTERS.put(BIGINT, ResultSet::getLong);

        GETTERS.put(VARCHAR, ResultSet::getString);

        GETTERS.put(REAL, ResultSet::getFloat);
        GETTERS.put(DOUBLE, ResultSet::getDouble);
        GETTERS.put(DECIMAL, ResultSet::getBigDecimal);

        GETTERS.put(DATE, (rs, columnIndex) -> rs.getObject(columnIndex, LocalDate.class));
        GETTERS.put(TIME, (rs, columnIndex) -> rs.getObject(columnIndex, LocalTime.class));
        GETTERS.put(TIMESTAMP, (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
        GETTERS.put(TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME,
                (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));
    }

    private GettersProvider() {
    }

}
