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

package com.hazelcast.sql;

import com.hazelcast.config.SqlConfig;
import com.hazelcast.internal.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Definition of an SQL statement.
 * <p>
 * This object is mutable. Properties are read once before the execution is started.
 * Changes to properties do not affect the behavior of already running statements.
 */
public final class SqlStatement {
    /** Value for the timeout that is not set. */
    public static final long TIMEOUT_NOT_SET = -1;

    /** Value for the timeout that is disabled. */
    public static final long TIMEOUT_DISABLED = 0;

    /** Default timeout. */
    public static final long DEFAULT_TIMEOUT = TIMEOUT_NOT_SET;

    /** Default cursor buffer size. */
    public static final int DEFAULT_CURSOR_BUFFER_SIZE = 4096;

    private String sql;
    private List<Object> parameters = new ArrayList<>();
    private long timeout = DEFAULT_TIMEOUT;
    private int cursorBufferSize = DEFAULT_CURSOR_BUFFER_SIZE;

    public SqlStatement(@Nonnull String sql) {
        setSql(sql);
    }

    /**
     * Copying constructor.
     */
    private SqlStatement(String sql, List<Object> parameters, long timeout, int cursorBufferSize) {
        this.sql = sql;
        this.parameters = parameters;
        this.timeout = timeout;
        this.cursorBufferSize = cursorBufferSize;
    }

    /**
     * Gets the SQL string to be executed.
     *
     * @return SQL string
     */
    @Nonnull
    public String getSql() {
        return sql;
    }

    /**
     * Sets the SQL string to be executed.
     * <p>
     * The SQL string cannot be null or empty.
     *
     * @param sql SQL string
     * @return this instance for chaining
     * @throws NullPointerException if passed SQL string is null
     * @throws IllegalArgumentException if passed SQL string is empty
     */
    @Nonnull
    public SqlStatement setSql(@Nonnull String sql) {
        Preconditions.checkNotNull(sql, "SQL cannot be null");

        if (sql.length() == 0) {
            throw new IllegalArgumentException("SQL cannot be empty");
        }

        this.sql = sql;

        return this;
    }

    /**
     * Gets the statement parameters.
     *
     * @return statement parameters
     */
    @Nonnull
    public List<Object> getParameters() {
        return parameters;
    }

    /**
     * Sets the statement parameters.
     * <p>
     * You may define parameter placeholders in the statement with the {@code "?"} character. For every placeholder, a parameter
     * value must be provided.
     * <p>
     * When the method is called, the content of the parameters list is copied. Subsequent changes to the original list don't
     * change the statement parameters.
     *
     * @param parameters statement parameters
     * @return this instance for chaining
     *
     * @see #addParameter(Object)
     * @see #clearParameters()
     */
    @Nonnull
    public SqlStatement setParameters(List<Object> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            this.parameters = new ArrayList<>();
        } else {
            this.parameters = new ArrayList<>(parameters);
        }

        return this;
    }

    /**
     * Adds a single parameter to the end of the parameters list.
     *
     * @param parameter parameter
     * @return this instance for chaining
     *
     * @see #setParameters(List)
     * @see #clearParameters()
     */
    @Nonnull
    public SqlStatement addParameter(Object parameter) {
        parameters.add(parameter);

        return this;
    }

    /**
     * Clears statement parameters.
     *
     * @return this instance for chaining
     *
     * @see #setParameters(List)
     * @see #addParameter(Object)
     */
    @Nonnull
    public SqlStatement clearParameters() {
        this.parameters = new ArrayList<>();

        return this;
    }

    /**
     * Gets the execution timeout in milliseconds.
     *
     * @return execution timeout in milliseconds
     */
    public long getTimeoutMillis() {
        return timeout;
    }

    /**
     * Sets the execution timeout in milliseconds.
     * <p>
     * If the timeout is reached for a running statement, it will be cancelled forcefully.
     * <p>
     * Zero value means no timeout. {@value #TIMEOUT_NOT_SET} means that the value from
     * {@link SqlConfig#getStatementTimeoutMillis()} will be used. Other negative values are prohibited.
     * <p>
     * Defaults to {@value #TIMEOUT_NOT_SET}.
     *
     * @param timeout execution timeout in milliseconds, {@code 0} for no timeout, {@code -1} to user member's default timeout
     * @return this instance for chaining
     *
     * @see SqlConfig#getStatementTimeoutMillis()
     */
    @Nonnull
    public SqlStatement setTimeoutMillis(long timeout) {
        if (timeout < 0 && timeout != TIMEOUT_NOT_SET) {
            throw new IllegalArgumentException("Timeout must be non-negative or -1: " + timeout);
        }

        this.timeout = timeout;

        return this;
    }

    /**
     * Gets the cursor buffer size (measured in the number of rows).
     *
     * @return cursor buffer size (measured in the number of rows)
     */
    public int getCursorBufferSize() {
        return cursorBufferSize;
    }

    /**
     * Sets the cursor buffer size (measured in the number of rows).
     * <p>
     * When a statement is submitted for execution, a {@link SqlResult} is returned as a result. When rows are ready to be
     * consumed, they are put into an internal buffer of the cursor. This parameter defines the maximum number of rows in
     * that buffer. When the threshold is reached, the backpressure mechanism will slow down the execution, possibly to a
     * complete halt, to prevent out-of-memory.
     * <p>
     * Only positive values are allowed.
     * <p>
     * The default value is expected to work well for the most workloads. A bigger buffer size may give you a slight performance
     * boost for queries with large result sets at the cost of increased memory consumption.
     * <p>
     * Defaults to {@value #DEFAULT_CURSOR_BUFFER_SIZE}.
     *
     * @param cursorBufferSize cursor buffer size (measured in the number of rows)
     * @return this instance for chaining
     *
     * @see SqlService#execute(SqlStatement)
     * @see SqlResult
     */
    @Nonnull
    public SqlStatement setCursorBufferSize(int cursorBufferSize) {
        if (cursorBufferSize <= 0) {
            throw new IllegalArgumentException("Cursor buffer size must be positive: " + cursorBufferSize);
        }

        this.cursorBufferSize = cursorBufferSize;

        return this;
    }

    /**
     * Creates a copy of this instance
     *
     * @return Copy of this instance
     */
    @Nonnull
    public SqlStatement copy() {
        return new SqlStatement(sql, new ArrayList<>(parameters), timeout, cursorBufferSize);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SqlStatement sqlStatement = (SqlStatement) o;

        return Objects.equals(sql, sqlStatement.sql)
            && Objects.equals(parameters, sqlStatement.parameters)
            && timeout == sqlStatement.timeout
            && cursorBufferSize == sqlStatement.cursorBufferSize;
    }

    @Override
    public int hashCode() {
        int result = sql != null ? sql.hashCode() : 0;

        result = 31 * result + parameters.hashCode();
        result = 31 * result + (int) (timeout ^ (timeout >>> 32));
        result = 31 * result + cursorBufferSize;

        return result;
    }

    @Override
    public String toString() {
        return "SqlStatement{"
            + "sql=" + sql
            + ", parameters=" + parameters
            + ", timeout=" + timeout
            + ", cursorBufferSize=" + cursorBufferSize
            + '}';
    }
}
