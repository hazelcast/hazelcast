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

import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Contains the configuration for an {@link DurableExecutorService}.
 */
public class DurableExecutorConfig implements IdentifiedDataSerializable, NamedConfig, Versioned {

    /**
     * The number of executor threads per Member for the Executor based on this configuration.
     */
    public static final int DEFAULT_POOL_SIZE = 16;

    /**
     * Capacity of RingBuffer (per partition).
     */
    public static final int DEFAULT_RING_BUFFER_CAPACITY = 100;

    /**
     * Durability of Executor.
     */
    public static final int DEFAULT_DURABILITY = 1;

    private String name = "default";

    private int poolSize = DEFAULT_POOL_SIZE;

    private int durability = DEFAULT_DURABILITY;

    private int capacity = DEFAULT_RING_BUFFER_CAPACITY;

    private String splitBrainProtectionName;

    private boolean statisticsEnabled = true;

    public DurableExecutorConfig() {
    }

    public DurableExecutorConfig(String name) {
        this.name = name;
    }

    public DurableExecutorConfig(String name, int poolSize, int durability, int capacity, boolean statisticsEnabled) {
        this(name, poolSize, durability, capacity, null, statisticsEnabled);
    }

    public DurableExecutorConfig(String name, int poolSize, int durability, int capacity,
                                 String splitBrainProtectionName, boolean statisticsEnabled) {
        this.name = name;
        this.poolSize = poolSize;
        this.durability = durability;
        this.capacity = capacity;
        this.splitBrainProtectionName = splitBrainProtectionName;
        this.statisticsEnabled = statisticsEnabled;
    }

    public DurableExecutorConfig(DurableExecutorConfig config) {
        this(config.getName(), config.getPoolSize(), config.getDurability(), config.getCapacity(),
                config.getSplitBrainProtectionName(), config.isStatisticsEnabled());
    }

    /**
     * Gets the name of the executor task.
     *
     * @return the name of the executor task
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the executor task.
     *
     * @param name the name of the executor task
     * @return this executor config instance
     */
    public DurableExecutorConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the number of executor threads per member for the executor.
     *
     * @return the number of executor threads per member for the executor
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Sets the number of executor threads per member for the executor.
     *
     * @param poolSize the number of executor threads per member for the executor
     * @return this executor config instance
     */
    public DurableExecutorConfig setPoolSize(int poolSize) {
        this.poolSize = checkPositive("poolSize", poolSize);
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
     * @return this executor config instance
     */
    public DurableExecutorConfig setDurability(int durability) {
        this.durability = checkNotNegative(durability, "durability can't be smaller than 0");
        return this;
    }

    /**
     * Gets the ring buffer capacity of the executor task.
     * This is a per partition parameter, so total capacity of the ringbuffers will be partitionCount * capacity
     *
     * @return Ring buffer capacity of the executor task
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Sets the ring buffer capacity of the executor task.
     *
     * @param capacity Ring Buffer capacity of the executor task
     * @return this executor config instance
     */
    public DurableExecutorConfig setCapacity(int capacity) {
        this.capacity = checkPositive("capacity", capacity);
        return this;
    }

    /**
     * Returns the split brain protection name for operations.
     *
     * @return the split brain protection name
     */
    public String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    /**
     * Sets the split brain protection name for operations.
     *
     * @param splitBrainProtectionName the split brain protection name
     * @return the updated configuration
     */
    public DurableExecutorConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
        return this;
    }

    /**
     * @return {@code true} if statistics gathering is enabled
     * on the executor task (default), {@code false} otherwise
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Enables or disables statistics gathering on the executor task.
     *
     * @param statisticsEnabled {@code true} if statistics
     *                          gathering is enabled on the executor task, {@code
     *                          false} otherwise @return this executor config instance
     */
    public DurableExecutorConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    @Override
    public String toString() {
        return "ExecutorConfig{"
                + "name='" + name + '\''
                + ", poolSize=" + poolSize
                + ", capacity=" + capacity
                + ", statisticsEnabled=" + statisticsEnabled
                + ", splitBrainProtectionName=" + splitBrainProtectionName
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.DURABLE_EXECUTOR_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(poolSize);
        out.writeInt(durability);
        out.writeInt(capacity);
        out.writeString(splitBrainProtectionName);
        out.writeBoolean(statisticsEnabled);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        poolSize = in.readInt();
        durability = in.readInt();
        capacity = in.readInt();
        splitBrainProtectionName = in.readString();
        statisticsEnabled = in.readBoolean();
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DurableExecutorConfig)) {
            return false;
        }

        DurableExecutorConfig that = (DurableExecutorConfig) o;
        if (poolSize != that.poolSize) {
            return false;
        }
        if (durability != that.durability) {
            return false;
        }
        if (capacity != that.capacity) {
            return false;
        }
        if (statisticsEnabled != that.statisticsEnabled) {
            return false;
        }
        if (!Objects.equals(splitBrainProtectionName, that.splitBrainProtectionName)) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public final int hashCode() {
        int result = name.hashCode();
        result = 31 * result + poolSize;
        result = 31 * result + durability;
        result = 31 * result + capacity;
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        result = 31 * result + (splitBrainProtectionName != null ? splitBrainProtectionName.hashCode() : 0);
        return result;
    }
}
