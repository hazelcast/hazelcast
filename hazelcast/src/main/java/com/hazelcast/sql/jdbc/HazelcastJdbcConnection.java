/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.jdbc;

import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.jdbc.impl.JdbcGateway;
import com.hazelcast.sql.jdbc.impl.JdbcUtils;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static com.hazelcast.sql.jdbc.impl.JdbcUtils.unsupported;

/**
 * JDBC connection.
 */
@SuppressWarnings({"RedundantThrows", "checkstyle:MethodCount"})
public class HazelcastJdbcConnection implements Connection {
    /** Gateway. */
    private final JdbcGateway gateway;

    /** Database metadata. */
    private final HazelcastJdbcDatabaseMetadata metadata;

    /** Closed flag. */
    private boolean closed;

    /** Read-only flag. JDBC driver doesn't use it except for the getter/setter. */
    private boolean readOnly = true;

    /** Auto-commit flag. */
    private boolean autoCommit = true;

    // TODO: Should be propagated from connection properties.
    /** Page size. */
    private int pageSize = SqlQuery.DEFAULT_CURSOR_BUFFER_SIZE;

    HazelcastJdbcConnection(JdbcGateway gateway) {
        this.gateway = gateway;

        metadata = new HazelcastJdbcDatabaseMetadata(this);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();

        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();

        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();

        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
    }

    @Override
    public void close() throws SQLException {
        if (!isClosed()) {
            gateway.close();

            closed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();

        return metadata;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();

        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();

        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();

        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        // TODO
    }

    @Override
    public String getSchema() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();

        return TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();

        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();

        throw unsupported("Type maps are not supported.");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkClosed();

        throw unsupported("Type maps are not supported.");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();

        if (metadata.supportsResultSetHoldability(holdability)) {
            throw new SQLException("Unsupported holdability: " + holdability);
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();

        return metadata.getResultSetHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw unsupportedSavepoints();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw unsupportedSavepoints();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw unsupportedSavepoints();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw unsupportedSavepoints();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkClosed();

        throw unsupported("prepareCall is not supported.");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        return prepareCall(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, getHoldability());
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        checkResultSetProperties(resultSetType, resultSetConcurrency, resultSetHoldability);

        return new HazelcastJdbcStatement(gateway, this, pageSize, false);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        checkClosed();
        checkResultSetProperties(resultSetType, resultSetConcurrency, resultSetHoldability);

        return new HazelcastJdbcPreparedStatement(gateway, this, pageSize, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();

        throw unsupportedAutoGeneratedKeys();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();

        throw unsupportedAutoGeneratedKeys();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkClosed();

        throw unsupportedAutoGeneratedKeys();
    }

    @Override
    public Clob createClob() throws SQLException {
        checkClosed();

        throw unsupported("Clobs are not supported.");
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkClosed();

        throw unsupported("Blobs are not supported.");
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();

        throw unsupported("NClobs are not supported.");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();

        throw unsupported("SQLXML is not supported.");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();

        throw unsupported("Structs are not supported.");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        // TODO: Create array of the given type. We may use Avatica implementation as reference.
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Timeout cannot be negative.");
        }

        // TODO: Check client instance for validity instead?
        return !isClosed();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return getClientInfo().getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();

        return new Properties();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw new SQLException("Executor cannot be null.");
        }

        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // TODO
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return JdbcUtils.isWrapperFor(this, iface);
    }

    public JdbcGateway getGateway() {
        return gateway;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed", "STATE", -1);
        }
    }

    private void checkResultSetProperties(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        if (!metadata.supportsResultSetType(resultSetType)) {
            throw new SQLException("Unsupported ResultSet type: " + resultSetType);
        }

        if (!metadata.supportsResultSetConcurrency(resultSetType, resultSetConcurrency)) {
            throw new SQLException("Unsupported ResultSet concurrency: " + resultSetConcurrency);
        }

        if (!metadata.supportsResultSetHoldability(resultSetHoldability)) {
            throw new SQLException("Unsupported ResultSet holdability: " + resultSetHoldability);
        }
    }

    private SQLFeatureNotSupportedException unsupportedAutoGeneratedKeys() throws SQLException {
        checkClosed();

        return unsupported("Auto-generated keys are not supported.");
    }

    private SQLFeatureNotSupportedException unsupportedSavepoints() throws SQLException {
        checkClosed();

        return unsupported("Savepoints are not supported.");
    }
}
