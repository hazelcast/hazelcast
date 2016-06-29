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

import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.spi.annotation.Beta;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Contains the configuration for an {@link DurableExecutorService}.
 */
@Beta
public class DurableExecutorConfig {

    /**
     * The number of executor threads per Member for the Executor based on this configuration.
     */
    public static final int DEFAULT_POOL_SIZE = 16;

    /**
     * Capacity of RingBuffer (per partition)
     */
    public static final int DEFAULT_RING_BUFFER_CAPACITY = 100;

    /**
     * Durability of Executor
     */
    public static final int DEFAULT_DURABILITY = 1;

    private String name = "default";

    private int poolSize = DEFAULT_POOL_SIZE;

    private int durability = DEFAULT_DURABILITY;

    private int capacity = DEFAULT_RING_BUFFER_CAPACITY;

    private DurableExecutorConfigReadOnly readOnly;

    public DurableExecutorConfig() {
    }

    public DurableExecutorConfig(String name) {
        this.name = name;
    }

    public DurableExecutorConfig(String name, int poolSize, int durability, int capacity) {
        this.name = name;
        this.poolSize = poolSize;
        this.durability = durability;
        this.capacity = capacity;
    }

    public DurableExecutorConfig(DurableExecutorConfig config) {
        this(config.getName(), config.getPoolSize(), config.getDurability(), config.getCapacity());
    }

    public DurableExecutorConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new DurableExecutorConfigReadOnly(this);
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
    public DurableExecutorConfig setName(String name) {
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
    public DurableExecutorConfig setPoolSize(int poolSize) {
        checkPositive(poolSize, "Pool size should be greater than 0");
        this.poolSize = poolSize;
        return this;
    }

    /**
     * Gets the durability of the executor
     *
     * @return the durability of the executor
     */
    public int getDurability() {
        return durability;
    }

    /**
     * Sets the durability of the executor
     *
     * @param durability the durability of the executor
     * @return This executor config instance.
     */
    public DurableExecutorConfig setDurability(int durability) {
        checkNotNegative(durability, "durability can't be smaller than 0");
        this.durability = durability;
        return this;
    }

    /**
     * Gets the ring buffer capacity of the executor task.
     * This is a per partition parameter, so total capacity of the ringbuffers will be partitionCount * capacity
     *
     * @return Ring buffer capacity of the executor task.
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Sets the ring buffer capacity of the executor task.
     *
     * @param capacity Ring Buffer capacity of the executor task.
     * @return This executor config instance.
     */
    public DurableExecutorConfig setCapacity(int capacity) {
        checkPositive(capacity, "Capacity should be greater than 0");
        this.capacity = capacity;
        return this;
    }

    @Override
    public String toString() {
        return "ExecutorConfig{"
                + "name='" + name + '\''
                + ", poolSize=" + poolSize
                + ", capacity=" + capacity
                + '}';
    }

    private static class DurableExecutorConfigReadOnly extends DurableExecutorConfig {

        public DurableExecutorConfigReadOnly(DurableExecutorConfig config) {
            super(config);
        }

        @Override
        public DurableExecutorConfig setName(String name) {
            throw new UnsupportedOperationException("This config is read-only durable executor: " + getName());
        }

        @Override
        public DurableExecutorConfig setPoolSize(int poolSize) {
            throw new UnsupportedOperationException("This config is read-only durable executor: " + getName());
        }

        @Override
        public DurableExecutorConfig setCapacity(int capacity) {
            throw new UnsupportedOperationException("This config is read-only durable executor: " + getName());
        }

        @Override
        public DurableExecutorConfig setDurability(int durability) {
            throw new UnsupportedOperationException("This config is read-only durable executor: " + getName());
        }
    }
}
