package com.hazelcast.sql.impl.parser;

import com.hazelcast.sql.impl.QueryException;

public class NoOpSqlParser implements SqlParser {

    @Override
    public Statement parse(SqlParseTask task) {
        throw QueryException.error("Cannot parse SQL because \"hazelcast-sql\" module is not in the classpath.");
    }
}
