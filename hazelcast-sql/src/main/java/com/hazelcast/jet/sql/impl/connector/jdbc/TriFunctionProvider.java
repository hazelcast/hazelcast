package com.hazelcast.jet.sql.impl.connector.jdbc;


import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.sql.impl.connector.jdbc.oracle.HazelcastOracleDialect;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

final class TriFunctionProvider {

    private static final Map<String, TriFunction<ResultSetMetaData, String, Integer, String>> dialectFunction = new HashMap<>();

    static {
        dialectFunction.put(HazelcastOracleDialect.class.getSimpleName(), HazelcastOracleDialect::isNumberTypeCheck);
    }

    private TriFunctionProvider(){}

    public static TriFunction<ResultSetMetaData, String, Integer, String> getTriFunction(String dialect) {
        return dialectFunction.getOrDefault(dialect, (rsMetaData, sqlType, col) -> sqlType);
    }
}
