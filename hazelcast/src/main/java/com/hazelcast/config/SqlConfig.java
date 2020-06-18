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

import com.hazelcast.sql.SqlQuery;

/**
 * SQL service configuration.
 */
public class SqlConfig {
    /** Default number of threads responsible for query processing. */
    public static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    /** Default number of threads responsible for network operations processing. */
    public static final int DEFAULT_OPERATION_THREAD_COUNT = Math.min(8, Runtime.getRuntime().availableProcessors());

    /** Default timeout in milliseconds that is applied to queries without explicit timeout. */
    public static final int DEFAULT_QUERY_TIMEOUT = 0;

    // TODO: Evaluate this default carefully.
    /** Default max memory. */
    public static final long DEFAULT_MAX_MEMORY = Runtime.getRuntime().maxMemory() / 2;

    /** Number of threads responsible for query processing. */
    private int threadCount = DEFAULT_THREAD_COUNT;

    /** Number of threads responsible for query operation handling. */
    private int operationThreadCount = DEFAULT_OPERATION_THREAD_COUNT;

    /** Timeout in milliseconds that is applied to queries without an explicit timeout. */
    private long queryTimeout = DEFAULT_QUERY_TIMEOUT;

    /** Max memory in bytes for SQL processing. */
    private long maxMemory = DEFAULT_MAX_MEMORY;

    /**
     * Gets the number of threads responsible for query processing.
     *
     * @return Number of threads responsible for query processing.
     */
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * Sets the number of threads responsible for query processing.
     * <p>
     * Hazelcast executes queries in parallel to achieve high throughput. This parameter defines the maximum number of threads
     * that may execute queries simultaneously. Normally the value of this parameter should be equal to the number of CPU cores.
     * <p>
     * Setting the value to less than the number of CPU cores will limit the degree of parallelism of the SQL subsystem. This
     * may be beneficial if you would like to prioritize other CPU-intensive workloads on the same machine.
     * <p>
     * It is not recommended to set the value of this parameter greater than the number of CPU cores because it may decrease
     * the system's overall performance due to excessive context switches.
     * <p>
     * Defaults to {@link #DEFAULT_THREAD_COUNT}.
     *
     * @param threadCount Number of threads responsible for query processing.
     * @return This instance for chaining.
     */
    public SqlConfig setThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

    /**
     * Gets the number of threads responsible for network operations processing.
     *
     * @return Number of threads responsible for network operations processing.
     */
    public int getOperationThreadCount() {
        return operationThreadCount;
    }

    /**
     * Sets the number of threads responsible for operation processing.
     * <p>
     * When Hazelcast members execute a query, they send commands to each other over the network to coordinate the execution.
     * This includes requests to start or stop query execution, or a request to process a batch of data. These commands are
     * processed in a separate operation thread pool, to avoid frequent interruption of running query fragments.
     * <p>
     * This parameter defines the number of threads in the operation thread pool. The default value should be good enough for
     * the most workloads. You may want to increase the default value if you run very small queries, or the machine has a big
     * number of CPU cores.
     * <p>
     * It is not recommended to set the value of this parameter greater than the number of CPU cores because it may decrease
     * the system's overall performance due to excessive context switches.
     *
     * <p>
     * Defaults to {@link #DEFAULT_OPERATION_THREAD_COUNT}.
     *
     * @param operationThreadCount Number of threads responsible for operation processing.
     * @return This instance for chaining.
     */
    public SqlConfig setOperationThreadCount(int operationThreadCount) {
        this.operationThreadCount = operationThreadCount;
        return this;
    }

    /**
     * Gets the timeout in milliseconds that is applied to queries without an explicit timeout.
     *
     * @return Timeout in milliseconds.
     */
    public long getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Sets the timeout in milliseconds that is applied to queries without an explicit timeout.
     * <p>
     * It is possible to set a query timeout through {@link SqlQuery#setTimeout(long)}. If the query timeout is not set, then
     * the value of this parameter will be used.
     * <p>
     * Zero value means no timeout. Negative values are prohibited.
     * <p>
     * Defaults to {@link #DEFAULT_QUERY_TIMEOUT}.
     *
     * @param queryTimeout Timeout in milliseconds.
     * @return This instance for chaining.
     */
    public SqlConfig setQueryTimeout(long queryTimeout) {
        this.queryTimeout = queryTimeout;

        return this;
    }

    /**
     * Gets the maximum amount of memory in bytes SQL engine is allowed to use for query processing. When the threshold is
     * reached, the engine may attempt to spill intermediate results to disk, or to cancel the query.
     * <p>
     * Defaults to {@link #DEFAULT_MAX_MEMORY}.
     *
     * @return Maximum amount of memory in bytes. Zero or negative value means no limit.
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * Set the maximum amount of memory in bytes SQL engine is allowed to use for query processing. When the threshold is
     * reached, the engine may attempt to spill intermediate results to disk, or to cancel the query.
     * <p>
     * Defaults to {@link #DEFAULT_MAX_MEMORY}.
     *
     * @param maxMemory Maximum amount of memory in bytes. Zero or negative value means no limit.
     * @return This instance for chaining.
     */
    public SqlConfig setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;

        return this;
    }

    @Override
    public String toString() {
        return "SqlConfig{threadCount=" + threadCount + ", operationThreadCount=" + operationThreadCount
            + ", queryTimeout=" + queryTimeout + ", maxMemory=" + maxMemory + '}';
    }
}
