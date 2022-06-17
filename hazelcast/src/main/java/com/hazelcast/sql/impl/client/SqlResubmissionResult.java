package com.hazelcast.sql.impl.client;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.sql.SqlRowMetadata;

class SqlResubmissionResult {
    private final Connection connection;
    private final SqlError sqlError;
    private final SqlRowMetadata rowMetadata;
    private final SqlPage rowPage;
    private final long updateCount;

    SqlResubmissionResult(SqlError sqlError) {
        this.sqlError = sqlError;
        this.connection = null;
        this.rowMetadata = null;
        this.rowPage = null;
        this.updateCount = 0;
    }

    SqlResubmissionResult(Connection connection, SqlRowMetadata rowMetadata, SqlPage rowPage, long updateCount) {
        this.connection = connection;
        this.rowMetadata = rowMetadata;
        this.rowPage = rowPage;
        this.updateCount = updateCount;
        this.sqlError = null;
    }

    Connection getConnection() {
        return connection;
    }

    SqlError getSqlError() {
        return sqlError;
    }

    SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }

    SqlPage getRowPage() {
        return rowPage;
    }

    long getUpdateCount() {
        return updateCount;
    }
}
