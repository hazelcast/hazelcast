package com.hazelcast.sql.impl.client;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.sql.SqlRowMetadata;

class SqlResubmissionResult {
    private final Connection connection;
    private final SqlError sqlError;
    private final SqlRowMetadata rowMetadata;
    private final SqlPage rowPage;
    private final long updateCount;

    public SqlResubmissionResult(SqlError sqlError) {
        this.sqlError = sqlError;
        this.connection = null;
        this.rowMetadata = null;
        this.rowPage = null;
        this.updateCount = 0;
    }

    public SqlResubmissionResult(Connection connection, SqlRowMetadata rowMetadata, SqlPage rowPage, long updateCount) {
        this.connection = connection;
        this.rowMetadata = rowMetadata;
        this.rowPage = rowPage;
        this.updateCount = updateCount;
        this.sqlError = null;
    }

    public Connection getConnection() {
        return connection;
    }

    public SqlError getSqlError() {
        return sqlError;
    }

    public SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }

    public SqlPage getRowPage() {
        return rowPage;
    }

    public long getUpdateCount() {
        return updateCount;
    }
}
