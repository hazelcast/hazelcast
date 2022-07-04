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

package com.hazelcast.config;

import com.hazelcast.sql.SqlStatement;

import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;

/**
 * SQL service configuration.
 */
public class SqlConfig {

    /** Default timeout in milliseconds that is applied to statements without explicit timeout. */
    public static final int DEFAULT_STATEMENT_TIMEOUT_MILLIS = 0;

    /** Timeout in milliseconds that is applied to statements without an explicit timeout. */
    private long statementTimeoutMillis = DEFAULT_STATEMENT_TIMEOUT_MILLIS;

    /**
     * Gets the timeout in milliseconds that is applied to statements without an explicit timeout.
     *
     * @return timeout in milliseconds
     */
    public long getStatementTimeoutMillis() {
        return statementTimeoutMillis;
    }

    /**
     * Sets the timeout in milliseconds that is applied to statements without an explicit timeout.
     * <p>
     * It is possible to set a timeout through the {@link SqlStatement#setTimeoutMillis(long)} method. If the statement
     * timeout is not set, then the value of this parameter will be used.
     * <p>
     * Zero value means no timeout. Negative values are prohibited.
     * <p>
     * Defaults to {@link #DEFAULT_STATEMENT_TIMEOUT_MILLIS}.
     *
     * @see SqlStatement#setTimeoutMillis(long)
     * @param statementTimeoutMillis timeout in milliseconds
     * @return this instance for chaining
     */
    public SqlConfig setStatementTimeoutMillis(long statementTimeoutMillis) {
        checkNotNegative(statementTimeoutMillis, "Timeout cannot be negative");

        this.statementTimeoutMillis = statementTimeoutMillis;

        return this;
    }

    @Override
    public String toString() {
        return "SqlConfig{"
            + "statementTimeoutMillis=" + statementTimeoutMillis
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlConfig sqlConfig = (SqlConfig) o;
        return statementTimeoutMillis == sqlConfig.statementTimeoutMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(statementTimeoutMillis);
    }
}
