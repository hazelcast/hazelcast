/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Configuration options for the {@link com.hazelcast.scheduledexecutor.IScheduledExecutorService}
 */
public class ScheduledExecutorConfig {

    /**
     * The number of executor threads per Member for the Executor based on this configuration.
     */
    private static final int DEFAULT_POOL_SIZE = 16;

    /**
     * The number of tasks that can co-exist per scheduler per partition
     */
    private static final int DEFAULT_CAPACITY = 100;

    /**
     * The number of replicas per task scheduled in each ScheduledExecutor
     */
    private static final int DEFAULT_DURABILITY = 1;

    private String name = "default";

    private int durability = DEFAULT_DURABILITY;

    private int capacity = DEFAULT_CAPACITY;

    private int poolSize = DEFAULT_POOL_SIZE;

    private ScheduledExecutorConfig.ScheduledExecutorConfigReadOnly readOnly;

    public ScheduledExecutorConfig() {
    }

    public ScheduledExecutorConfig(String name) {
        this.name = name;
    }

    public ScheduledExecutorConfig(String name, int durability, int capacity, int poolSize) {
        this.name = name;
        this.durability = durability;
        this.poolSize = poolSize;
        this.capacity = capacity;
    }

    public ScheduledExecutorConfig(ScheduledExecutorConfig config) {
        this(config.getName(), config.getDurability(), config.getCapacity(), config.getPoolSize());
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
    public ScheduledExecutorConfig setName(String name) {
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
    public ScheduledExecutorConfig setPoolSize(int poolSize) {
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
     * The durability represents the number of replicas that exist in a cluster for any given partition-owned task.
     * If this is set to 0 then there is only 1 copy of the task in the cluster, meaning that if the partition owning it, goes
     * down then the task is lost.
     *
     * @param durability the durability of the executor
     * @return This executor config instance.
     */
    public ScheduledExecutorConfig setDurability(int durability) {
        checkNotNegative(durability, "durability can't be smaller than 0");
        this.durability = durability;
        return this;
    }

    /**
     * Gets the capacity of the executor
     *
     * @return the capacity of the executor
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Sets the capacity of the executor
     * The capacity represents the maximum number of tasks that a scheduler can have at any given point in time per partition.
     * If this is set to 0 then there is no limit
     *
     * @param capacity the capacity of the executor
     * @return This executor config instance.
     */
    public ScheduledExecutorConfig setCapacity(int capacity) {
        checkNotNegative(capacity, "capacity can't be smaller than 0");
        this.capacity = capacity;
        return this;
    }

    @Override
    public String toString() {
        return "ScheduledExecutorConfig{"
                + "name='" + name + '\''
                + ", durability=" + durability
                + ", poolSize-" + poolSize
                + ", capacity-" + capacity
                + '}';
    }

    ScheduledExecutorConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new ScheduledExecutorConfig.ScheduledExecutorConfigReadOnly(this);
        }
        return readOnly;
    }

    private static class ScheduledExecutorConfigReadOnly extends ScheduledExecutorConfig {

        ScheduledExecutorConfigReadOnly(ScheduledExecutorConfig config) {
            super(config);
        }

        @Override
        public ScheduledExecutorConfig setName(String name) {
            throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
        }

        @Override
        public ScheduledExecutorConfig setDurability(int durability) {
            throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
        }

        @Override
        public ScheduledExecutorConfig setPoolSize(int poolSize) {
            throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
        }

        @Override
        public ScheduledExecutorConfig setCapacity(int capacity) {
            throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
        }
    }
}
