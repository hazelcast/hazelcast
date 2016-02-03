/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Contains the configuration for an {@link com.hazelcast.core.IExecutorService}.
 */
public class ExecutorConfig {

    /**
     * The number of executor threads per Member for the Executor based on this configuration.
     */
    public static final int DEFAULT_POOL_SIZE = 16;

    /**
     * Capacity of Queue
     */
    public static final int DEFAULT_QUEUE_CAPACITY = Integer.MAX_VALUE;

    private String name = "default";

    private int poolSize = DEFAULT_POOL_SIZE;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private boolean statisticsEnabled = true;

    private ExecutorConfigReadOnly readOnly;

    public ExecutorConfig() {
    }

    public ExecutorConfig(String name) {
        this.name = name;
    }

    public ExecutorConfig(String name, int poolSize) {
        this.name = name;
        this.poolSize = poolSize;
    }

    public ExecutorConfig(ExecutorConfig config) {
        this.name = config.name;
        this.poolSize = config.poolSize;
        this.queueCapacity = config.queueCapacity;
        this.statisticsEnabled = config.statisticsEnabled;
    }

    public ExecutorConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new ExecutorConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of the executor task.
     *
     * @return The name of the executor task.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the executor task.
     *
     * @param name The name of the executor task.
     * @return This executor config instance.
     */
    public ExecutorConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the number of executor threads per member for the executor.
     *
     * @return The number of executor threads per member for the executor.
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Sets the number of executor threads per member for the executor.
     *
     * @param poolSize The number of executor threads per member for the executor.
     * @return This executor config instance.
     */
    public ExecutorConfig setPoolSize(final int poolSize) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("poolSize must be positive");
        }
        this.poolSize = poolSize;
        return this;
    }

    /**
     * Gets the queue capacity of the executor task. 0 means Integer.MAX_VALUE.
     *
     * @return Queue capacity of the executor task. 0 means Integer.MAX_VALUE.
     */
    public int getQueueCapacity() {
        return queueCapacity;
    }

    /**
     * Sets the queue capacity of the executor task. 0 means Integer.MAX_VALUE.
     *
     * @param queueCapacity Queue capacity of the executor task. 0 means Integer.MAX_VALUE.
     * @return This executor config instance.
     */
    public ExecutorConfig setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        return this;
    }

    /**
     * Gets if statistics gathering is enabled or disabled on the executor task.
     *
     * @return True (default) if statistics gathering is enabled on the executor task, false otherwise.
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Enables or disables statistics gathering on the executor task.
     *
     * @param statisticsEnabled True (default) if statistics gathering is enabled on the executor task, false otherwise.
     * @return This executor config instance.
     */
    public ExecutorConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    @Override
    public String toString() {
        return "ExecutorConfig{"
                + "name='" + name + '\''
                + ", poolSize=" + poolSize
                + ", queueCapacity=" + queueCapacity
                + '}';
    }
}
