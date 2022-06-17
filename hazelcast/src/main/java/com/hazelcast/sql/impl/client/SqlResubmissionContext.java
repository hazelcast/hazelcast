package com.hazelcast.sql.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.sql.SqlStatement;

class SqlResubmissionContext {
    private final ClientMessage sqlExecuteMessage;
    private final boolean selectQuery;

    SqlResubmissionContext(ClientMessage sqlExecuteMessage, SqlStatement statement) {
        this.sqlExecuteMessage = sqlExecuteMessage;
        this.selectQuery = isSelectQuery(statement);
    }

    private boolean isSelectQuery(SqlStatement sqlStatement) {
        return sqlStatement.getSql().trim().toLowerCase().startsWith("select");
    }

    ClientMessage getSqlExecuteMessage() {
        return sqlExecuteMessage;
    }

    boolean isSelectQuery() {
        return selectQuery;
    }
}
