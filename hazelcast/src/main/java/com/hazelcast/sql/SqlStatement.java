/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import javax.annotation.Nullable;
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
    /**
     * Value for the timeout that is not set. The value of {@link
     * SqlConfig#setStatementTimeoutMillis} will be used.
     */
    public static final long TIMEOUT_NOT_SET = -1;

    /**
     * Value for the timeout that is disabled, meaning there's no time limit to
     * run a query.
     */
    public static final long TIMEOUT_DISABLED = 0;

    /** Default timeout. */
    public static final long DEFAULT_TIMEOUT = TIMEOUT_NOT_SET;

    /** Default cursor buffer size. */
    public static final int DEFAULT_CURSOR_BUFFER_SIZE = 4096;

    private String sql;
    private List<Object> arguments = new ArrayList<>();
    private long timeout = DEFAULT_TIMEOUT;
    private int cursorBufferSize = DEFAULT_CURSOR_BUFFER_SIZE;
    private String schema;
    private SqlExpectedResultType expectedResultType = SqlExpectedResultType.ANY;

    public SqlStatement(@Nonnull String sql) {
        setSql(sql);
    }

    /**
     * Copying constructor.
     */
    private SqlStatement(
        String sql,
        List<Object> arguments,
        long timeout,
        int cursorBufferSize,
        String schema,
        SqlExpectedResultType expectedResultType
    ) {
        this.sql = sql;
        this.arguments = arguments;
        this.timeout = timeout;
        this.cursorBufferSize = cursorBufferSize;
        this.schema = schema;
        this.expectedResultType = expectedResultType;
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
     * Gets the schema name.
     *
     * @return the schema name or {@code null} if there is none
     * @since 4.2
     */
    @Nullable
    public String getSchema() {
        return schema;
    }

    /**
     * Sets the schema name. The engine will try to resolve the non-qualified
     * object identifiers from the statement in the given schema. If not found, the default
     * search path will be used, which looks for objects in the predefined schemas {@code "partitioned"}
     * and {@code "public"}.
     * <p>
     * The schema name is case sensitive. For example, {@code "foo"} and {@code "Foo"} are different schemas.
     * <p>
     * The default value is {@code null} meaning only the default search path is used.
     *
     * @param schema the current schema name
     * @return this instance for chaining
     * @since 4.2
     */
    @Nonnull
    public SqlStatement setSchema(@Nullable String schema) {
        this.schema = schema;

        return this;
    }

    /**
     * Gets the statement parameter values.
     */
    @Nonnull
    public List<Object> getParameters() {
        return arguments;
    }

    /**
     * Sets the values for statement parameters.
     * <p>
     * You may define parameter placeholders in the statement with the {@code "?"} character. For every placeholder, a
     * value must be provided.
     * <p>
     * When the method is called, the contents of the list are copied. Subsequent changes to the original list don't
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
            this.arguments = new ArrayList<>();
        } else {
            this.arguments = new ArrayList<>(parameters);
        }

        return this;
    }

    /**
     * Adds a single parameter value to the end of the parameter values list.
     *
     * @param value parameter value
     * @return this instance for chaining
     *
     * @see #setParameters(List)
     * @see #clearParameters()
     */
    @Nonnull
    public SqlStatement addParameter(Object value) {
        arguments.add(value);

        return this;
    }

    /**
     * Clears statement parameter values.
     *
     * @return this instance for chaining
     *
     * @see #setParameters(List)
     * @see #addParameter(Object)
     */
    @Nonnull
    public SqlStatement clearParameters() {
        this.arguments.clear();

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
     * The default value is expected to work well for most workloads. A bigger buffer size may give you a slight performance
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
     * Gets the expected result type.
     *
     * @return expected result type
     * @since 4.2
     */
    @Nonnull
    public SqlExpectedResultType getExpectedResultType() {
        return expectedResultType;
    }

    /**
     * Sets the expected result type.
     *
     * @param expectedResultType expected result type
     * @return this instance for chaining
     * @since 4.2
     */
    @Nonnull
    public SqlStatement setExpectedResultType(@Nonnull SqlExpectedResultType expectedResultType) {
        Preconditions.checkNotNull(expectedResultType, "Expected result type cannot be null");

        this.expectedResultType = expectedResultType;

        return this;
    }

    /**
     * Creates a copy of this instance
     *
     * @return Copy of this instance
     */
    @Nonnull
    public SqlStatement copy() {
        return new SqlStatement(sql, new ArrayList<>(arguments), timeout, cursorBufferSize, schema, expectedResultType);
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
            && Objects.equals(arguments, sqlStatement.arguments)
            && timeout == sqlStatement.timeout
            && cursorBufferSize == sqlStatement.cursorBufferSize
            && Objects.equals(schema, sqlStatement.schema)
            && expectedResultType == sqlStatement.expectedResultType;
    }

    @Override
    public int hashCode() {
        int result = sql != null ? sql.hashCode() : 0;

        result = 31 * result + arguments.hashCode();
        result = 31 * result + (int) (timeout ^ (timeout >>> 32));
        result = 31 * result + cursorBufferSize;
        result = 31 * result + (schema != null ? schema.hashCode() : 0);
        result = 31 * result + expectedResultType.ordinal();

        return result;
    }

    @Override
    public String toString() {
        return "SqlStatement{"
            + "schema=" + schema
            + ", sql=" + sql
            + ", arguments=" + arguments
            + ", timeout=" + timeout
            + ", cursorBufferSize=" + cursorBufferSize
            + ", expectedResultType=" + expectedResultType
            + '}';
    }
}
