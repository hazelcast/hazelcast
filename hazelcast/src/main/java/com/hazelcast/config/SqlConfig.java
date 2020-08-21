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

package com.hazelcast.config;

import com.hazelcast.sql.SqlStatement;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * SQL service configuration.
 */
public class SqlConfig {
    /** Default number of threads responsible for execution of SQL statements. */
    public static final int DEFAULT_EXECUTOR_POOL_SIZE = -1;

    /** Default number of threads responsible for network operations processing. */
    public static final int DEFAULT_OPERATION_POOL_SIZE = -1;

    /** Default timeout in milliseconds that is applied to statements without explicit timeout. */
    public static final int DEFAULT_STATEMENT_TIMEOUT_MILLIS = 0;

    /** Number of threads responsible for execution of SQL statements. */
    private int executorPoolSize = DEFAULT_EXECUTOR_POOL_SIZE;

    /** Number of threads responsible for network operations processing. */
    private int operationPoolSize = DEFAULT_OPERATION_POOL_SIZE;

    /** Timeout in milliseconds that is applied to statements without an explicit timeout. */
    private long statementTimeoutMillis = DEFAULT_STATEMENT_TIMEOUT_MILLIS;

    /**
     * Gets the number of threads responsible for execution of SQL statements.
     *
     * @return number of threads responsible for execution of SQL statements
     */
    public int getExecutorPoolSize() {
        return executorPoolSize;
    }

    /**
     * Sets the number of threads responsible for execution of SQL statements.
     * <p>
     * The default value {@code -1} sets the pool size equal to the number of CPU cores, and should be good enough
     * for the most workloads.
     * <p>
     * Setting the value to less than the number of CPU cores will limit the degree of parallelism of the SQL subsystem. This
     * may be beneficial if you would like to prioritize other CPU-intensive workloads on the same machine.
     * <p>
     * It is not recommended to set the value of this parameter greater than the number of CPU cores because it may decrease
     * the system's overall performance due to excessive context switches.
     * <p>
     * Defaults to {@code -1}.
     *
     * @param executorPoolSize number of threads responsible for execution of SQL statements
     * @return this instance for chaining
     */
    public SqlConfig setExecutorPoolSize(int executorPoolSize) {
        if (executorPoolSize < DEFAULT_EXECUTOR_POOL_SIZE || executorPoolSize == 0) {
            checkPositive(executorPoolSize, "Executor pool size should be positive or -1: " + executorPoolSize);
        }

        this.executorPoolSize = executorPoolSize;

        return this;
    }

    /**
     * Gets the number of threads responsible for network operations processing.
     *
     * @return number of threads responsible for network operations processing
     */
    public int getOperationPoolSize() {
        return operationPoolSize;
    }

    /**
     * Sets the number of threads responsible for network operations processing.
     * <p>
     * When Hazelcast members execute an SQL statement, they send commands to each other over the network to coordinate the
     * execution. This includes requests to start or stop execution, or a request to process a batch of data. These
     * commands are processed in a separate operation thread pool, to avoid frequent interruption of running SQL fragments.
     * <p>
     * The default value {@code -1} sets the pool size equal to the number of CPU cores, and should be good enough
     * for the most workloads.
     * <p>
     * Setting the value to less than the number of CPU cores may improve the overall performance on machines
     * with large CPU count, because it will decrease the number of context switches.
     * <p>
     * It is not recommended to set the value of this parameter greater than the number of CPU cores because it
     * may decrease the system's overall performance due to excessive context switches.
     * <p>
     * Defaults to {@code -1}.
     *
     * @param operationPoolSize number of threads responsible for network operations processing
     * @return this instance for chaining
     */
    public SqlConfig setOperationPoolSize(int operationPoolSize) {
        if (operationPoolSize < DEFAULT_OPERATION_POOL_SIZE || operationPoolSize == 0) {
            checkPositive(operationPoolSize, "Operation pool size should be positive or -1: " + operationPoolSize);
        }

        this.operationPoolSize = operationPoolSize;

        return this;
    }

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
            + "executorPoolSize=" + executorPoolSize
            + ", operationPoolSize=" + operationPoolSize
            + ", statementTimeoutMillis=" + statementTimeoutMillis
            + '}';
    }
}
