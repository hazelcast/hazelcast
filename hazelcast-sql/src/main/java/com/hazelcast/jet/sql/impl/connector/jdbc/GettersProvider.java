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
import com.hazelcast.jet.sql.impl.connector.jdbc.mysql.HazelcastMySqlDialect;
import org.apache.calcite.sql.dialect.H2SqlDialect;
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

        //Mapping for MSSQL
        Map<String, BiFunctionEx<ResultSet, Integer, Object>> msSql = new HashMap<>(GETTERS);
        msSql.put("FLOAT", ResultSet::getDouble);
        msSql.put("DATETIME", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
        msSql.put("DATETIMEOFFSET", (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));
        GETTERSBYDATABASE.put(HazelcastMSSQLDialect.class.getSimpleName(), msSql);

        //Mapping for H2, Mysql and Postgre
        Map<String, BiFunctionEx<ResultSet, Integer, Object>> mySql = new HashMap<>(GETTERS);
        mySql.put("FLOAT", ResultSet::getFloat);
        mySql.put("TIMESTAMP", (rs, columnIndex) -> rs.getObject(columnIndex, LocalDateTime.class));
        mySql.put("TIMESTAMP_WITH_TIMEZONE", (rs, columnIndex) -> rs.getObject(columnIndex, OffsetDateTime.class));
        GETTERSBYDATABASE.put(HazelcastMySqlDialect.class.getSimpleName(), mySql);

        GETTERSBYDATABASE.put(PostgresqlSqlDialect.class.getSimpleName(), mySql);
        GETTERSBYDATABASE.put(H2SqlDialect.class.getSimpleName(), mySql);

    }

    public GettersProvider() {}

    public Map<String, BiFunctionEx<ResultSet, Integer, Object>> getGETTERS(String dialect) {
        if (dialect.equals(H2SqlDialect.class.getSimpleName())) {
            return GETTERSBYDATABASE.get(H2SqlDialect.class.getSimpleName());
        }
        if (dialect.equals(HazelcastMSSQLDialect.class.getSimpleName())) {
            return GETTERSBYDATABASE.get(HazelcastMSSQLDialect.class.getSimpleName());
        }
        if (dialect.equals(HazelcastMySqlDialect.class.getSimpleName())) {
            return GETTERSBYDATABASE.get(HazelcastMySqlDialect.class.getSimpleName());
        }
        if (dialect.equals(PostgresqlSqlDialect.class.getSimpleName())) {
            return GETTERSBYDATABASE.get(PostgresqlSqlDialect.class.getSimpleName());
        }
        return GETTERSBYDATABASE.get("DEFAULT");
    }
}
