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


import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.sql.impl.connector.jdbc.oracle.HazelcastOracleDialect;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

final class DatabaseSpecificTypeCheck {

    private static final Map<String, TriFunction<ResultSetMetaData, String, Integer, String>> DIALECT_TRIFUNC = new HashMap<>();

    static {
        DIALECT_TRIFUNC.put(HazelcastOracleDialect.class.getSimpleName(), HazelcastOracleDialect::isNumberTypeCheck);
    }

    private DatabaseSpecificTypeCheck() { }

    public static TriFunction<ResultSetMetaData, String, Integer, String> getTypeCheck(String dialect) {
        return DIALECT_TRIFUNC.getOrDefault(dialect, (rsMetaData, sqlType, col) -> sqlType);
    }
}
