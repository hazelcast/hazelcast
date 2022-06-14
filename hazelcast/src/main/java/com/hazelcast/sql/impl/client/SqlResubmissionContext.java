package com.hazelcast.sql.impl.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.sql.SqlStatement;

class SqlResubmissionContext {
    private final ClientMessage requestMessage;
    private final boolean selectQuery;

    SqlResubmissionContext(ClientMessage requestMessage, SqlStatement statement) {
        this.requestMessage = requestMessage;
        this.selectQuery = isSelectQuery(statement);
    }

    private boolean isSelectQuery(SqlStatement sqlStatement) {
        // TODO make it faster
        return sqlStatement.getSql().trim().toLowerCase().startsWith("select");
    }

    ClientMessage getRequestMessage() {
        return requestMessage;
    }

    boolean isSelectQuery() {
        return selectQuery;
    }
}
