/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Contains the configuration for an {@link DurableExecutorService}.
 */
public class DurableExecutorConfig implements IdentifiedDataSerializable, Versioned, NamedConfig {

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

    private String quorumName;

    private transient DurableExecutorConfigReadOnly readOnly;

    public DurableExecutorConfig() {
    }

    public DurableExecutorConfig(String name) {
        this.name = name;
    }

    public DurableExecutorConfig(String name, int poolSize, int durability, int capacity) {
        this(name, poolSize, durability, capacity, null);
    }

    public DurableExecutorConfig(String name, int poolSize, int durability, int capacity, String quorumName) {
        this.name = name;
        this.poolSize = poolSize;
        this.durability = durability;
        this.capacity = capacity;
        this.quorumName = quorumName;
    }

    public DurableExecutorConfig(DurableExecutorConfig config) {
        this(config.getName(), config.getPoolSize(), config.getDurability(), config.getCapacity(), config.getQuorumName());
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
        this.poolSize = checkPositive(poolSize, "Pool size should be greater than 0");
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
        this.capacity = checkPositive(capacity, "Capacity should be greater than 0");
        return this;
    }

    /**
     * Returns the quorum name for operations.
     *
     * @return the quorum name
     */
    public String getQuorumName() {
        return quorumName;
    }

    /**
     * Sets the quorum name for operations.
     *
     * @param quorumName the quorum name
     * @return the updated configuration
     */
    public DurableExecutorConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }


    @Override
    public String toString() {
        return "ExecutorConfig{"
                + "name='" + name + '\''
                + ", poolSize=" + poolSize
                + ", capacity=" + capacity
                + ", quorumName=" + quorumName
                + '}';
    }

    DurableExecutorConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new DurableExecutorConfigReadOnly(this);
        }
        return readOnly;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.DURABLE_EXECUTOR_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(poolSize);
        out.writeInt(durability);
        out.writeInt(capacity);
        out.writeUTF(quorumName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        poolSize = in.readInt();
        durability = in.readInt();
        capacity = in.readInt();
        quorumName = in.readUTF();
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
        if (quorumName != null ? !quorumName.equals(that.quorumName) : that.quorumName != null) {
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
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        return result;
    }

    // not private for testing
    static class DurableExecutorConfigReadOnly extends DurableExecutorConfig {

        DurableExecutorConfigReadOnly(DurableExecutorConfig config) {
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

        @Override
        public DurableExecutorConfig setQuorumName(String quorumName) {
            throw new UnsupportedOperationException("This config is read-only durable executor: " + getName());
        }
    }
}
