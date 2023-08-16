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
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.H2SqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;



public class GettersProvider {

    private static Map<String, Map<String, BiFunctionEx<ResultSet, Integer, Object>>> GETTERSBYDATABASE = new HashMap<>();
    private static HashMap<String, BiFunctionEx<ResultSet, Integer, Object>> GETTERS = new HashMap<>();

    static {
        GETTERS.put("BOOLEAN", ResultSet::getBoolean);
        GETTERS.put("BOOL", ResultSet::getBoolean);
        GETTERS.put("BIT", ResultSet::getBoolean);

        GETTERS.put("TINYINT", ResultSet::getByte);

        GETTERS.put("SMALLINT", ResultSet::getShort);
        GETTERS.put("INT2", ResultSet::getShort);

        GETTERS.put("INT", ResultSet::getInt);
        GETTERS.put("INT4", ResultSet::getInt);
        GETTERS.put("INTEGER", ResultSet::getInt);

        GETTERS.put("INT8", ResultSet::getLong);
        GETTERS.put("BIGINT", ResultSet::getLong);

        GETTERS.put("VARCHAR", ResultSet::getString);
        GETTERS.put("CHARACTER VARYING", ResultSet::getString);
        GETTERS.put("TEXT", ResultSet::getString);

        GETTERS.put("REAL", ResultSet::getFloat);
        GETTERS.put("FLOAT4", ResultSet::getFloat);

        GETTERS.put("DOUBLE", ResultSet::getDouble);
        GETTERS.put("DOUBLE PRECISION", ResultSet::getDouble);
        GETTERS.put("DECIMAL", ResultSet::getBigDecimal);
        GETTERS.put("NUMERIC", ResultSet::getBigDecimal);

        GETTERS.put("DATE", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDate.class));
        GETTERS.put("TIME", (rs, columnIndex) -> rs.getObject(columnIndex, LocalTime.class));

        GETTERSBYDATABASE.put("DEFAULT", GETTERS);

        GETTERSBYDATABASE.put("MICROSOFT SQL SERVER", (HashMap<String, BiFunctionEx<ResultSet, Integer, Object>>) GETTERS.clone());
        GETTERSBYDATABASE.put("MYSQL", (HashMap<String, BiFunctionEx<ResultSet, Integer, Object>>) GETTERS.clone());
        GETTERSBYDATABASE.put("POSTGRESQL", (HashMap<String, BiFunctionEx<ResultSet, Integer, Object>>) GETTERS.clone());
        GETTERSBYDATABASE.put("H2", (HashMap<String, BiFunctionEx<ResultSet, Integer, Object>>) GETTERS.clone());

        GETTERSBYDATABASE.get("MICROSOFT SQL SERVER").put("FLOAT", ResultSet::getDouble);
        GETTERSBYDATABASE.get("MICROSOFT SQL SERVER").put("DATETIME", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
        GETTERSBYDATABASE.get("MICROSOFT SQL SERVER").put("DATETIMEOFFSET", (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));

        GETTERSBYDATABASE.get("H2").put("FLOAT", ResultSet::getFloat);
        GETTERSBYDATABASE.get("H2").put("TIMESTAMP", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
        GETTERSBYDATABASE.get("H2").put("TIMESTAMP_WITH_TIMEZONE", (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));

        GETTERSBYDATABASE.get("MYSQL").put("FLOAT", ResultSet::getFloat);
        GETTERSBYDATABASE.get("MYSQL").put("TIMESTAMP", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
        GETTERSBYDATABASE.get("MYSQL").put("TIMESTAMP_WITH_TIMEZONE", (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));

        GETTERSBYDATABASE.get("POSTGRESQL").put("FLOAT", ResultSet::getFloat);
        GETTERSBYDATABASE.get("POSTGRESQL").put("TIMESTAMP", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
        GETTERSBYDATABASE.get("POSTGRESQL").put("TIMESTAMP_WITH_TIMEZONE", (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));
    }

    public GettersProvider() {}

    public Map<String, BiFunctionEx<ResultSet, Integer, Object>> getGETTERS(String dialect) {
        if (dialect.equals("H2")) {
            return GETTERSBYDATABASE.get("H2");
        }
        if (dialect.equals("MICROSOFT SQL SERVER")) {
            return GETTERSBYDATABASE.get("MICROSOFT SQL SERVER");
        }
        if (dialect.equals("MYSQL")) {
            return GETTERSBYDATABASE.get("MYSQL");
        }
        if (dialect.equals("POSTGRESQL")) {
            return GETTERSBYDATABASE.get("POSTGRESQL");
        }
        return GETTERSBYDATABASE.get("DEFAULT");
    }
}
