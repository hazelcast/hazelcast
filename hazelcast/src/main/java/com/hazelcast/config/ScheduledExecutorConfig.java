/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.merge.SplitBrainMergeTypeProvider;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Configuration options for the {@link com.hazelcast.scheduledexecutor.IScheduledExecutorService}.
 */
public class ScheduledExecutorConfig implements SplitBrainMergeTypeProvider, IdentifiedDataSerializable, Versioned {

    /**
     * The number of executor threads per Member for the Executor based on this configuration.
     */
    private static final int DEFAULT_POOL_SIZE = 16;

    /**
     * The number of tasks that can co-exist per scheduler per partition.
     */
    private static final int DEFAULT_CAPACITY = 100;

    /**
     * The number of replicas per task scheduled in each ScheduledExecutor.
     */
    private static final int DEFAULT_DURABILITY = 1;

    private String name = "default";

    private int durability = DEFAULT_DURABILITY;

    private int capacity = DEFAULT_CAPACITY;

    private int poolSize = DEFAULT_POOL_SIZE;

    private String quorumName;

    private transient ScheduledExecutorConfig.ScheduledExecutorConfigReadOnly readOnly;

    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();

    public ScheduledExecutorConfig() {
    }

    public ScheduledExecutorConfig(String name) {
        this.name = name;
    }

    public ScheduledExecutorConfig(String name, int durability, int capacity, int poolSize) {
        this(name, durability, capacity, poolSize, null, new MergePolicyConfig());
    }

    public ScheduledExecutorConfig(String name, int durability, int capacity, int poolSize, String quorumName,
                                   MergePolicyConfig mergePolicyConfig) {
        this.name = name;
        this.durability = durability;
        this.poolSize = poolSize;
        this.capacity = capacity;
        this.quorumName = quorumName;
        this.mergePolicyConfig = mergePolicyConfig;
    }

    public ScheduledExecutorConfig(ScheduledExecutorConfig config) {
        this(config.getName(), config.getDurability(), config.getCapacity(), config.getPoolSize(), config.getQuorumName(),
                config.getMergePolicyConfig());
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
    public ScheduledExecutorConfig setName(String name) {
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
    public ScheduledExecutorConfig setPoolSize(int poolSize) {
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
     * The durability represents the number of replicas that exist in a cluster for any given partition-owned task.
     * If this is set to 0 then there is only 1 copy of the task in the cluster, meaning that if the partition owning it, goes
     * down then the task is lost.
     *
     * @param durability the durability of the executor
     * @return this executor config instance
     */
    public ScheduledExecutorConfig setDurability(int durability) {
        this.durability = checkNotNegative(durability, "durability can't be smaller than 0");
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
     * @return this executor config instance
     */
    public ScheduledExecutorConfig setCapacity(int capacity) {
        this.capacity = checkNotNegative(capacity, "capacity can't be smaller than 0");
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
    public ScheduledExecutorConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }


    /**
    * Gets the {@link MergePolicyConfig} for the scheduler.
    *
    * @return the {@link MergePolicyConfig} for the scheduler
    */
    public MergePolicyConfig getMergePolicyConfig() {
        return mergePolicyConfig;
    }

    /**
    * Sets the {@link MergePolicyConfig} for the scheduler.
    *
    * @return this executor config instance
    */
    public ScheduledExecutorConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = checkNotNull(mergePolicyConfig, "mergePolicyConfig cannot be null");
        return this;
    }

    @Override
    public Class getProvidedMergeTypes() {
        return SplitBrainMergeTypes.ScheduledExecutorMergeTypes.class;
    }

    @Override
    public String toString() {
        return "ScheduledExecutorConfig{"
                + "name='" + name + '\''
                + ", durability=" + durability
                + ", poolSize=" + poolSize
                + ", capacity=" + capacity
                + ", quorumName=" + quorumName
                + ", mergePolicyConfig=" + mergePolicyConfig
                + '}';
    }

    ScheduledExecutorConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new ScheduledExecutorConfig.ScheduledExecutorConfigReadOnly(this);
        }
        return readOnly;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.SCHEDULED_EXECUTOR_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(durability);
        out.writeInt(capacity);
        out.writeInt(poolSize);
        // RU_COMPAT_3_9
        if (out.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            out.writeUTF(quorumName);
            out.writeObject(mergePolicyConfig);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        durability = in.readInt();
        capacity = in.readInt();
        poolSize = in.readInt();
        // RU_COMPAT_3_9
        if (in.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            quorumName = in.readUTF();
            mergePolicyConfig = in.readObject();
        }
    }

    @SuppressWarnings({"checkstyle:npathcomplexity"})
    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof ScheduledExecutorConfig)) {
            return false;
        }
        ScheduledExecutorConfig that = (ScheduledExecutorConfig) o;

        if (durability != that.durability) {
            return false;
        }
        if (capacity != that.capacity) {
            return false;
        }
        if (poolSize != that.poolSize) {
            return false;
        }
        if (quorumName != null ? !quorumName.equals(that.quorumName) : that.quorumName != null) {
            return false;
        }
        if (mergePolicyConfig != null ? !mergePolicyConfig.equals(that.mergePolicyConfig) : that.mergePolicyConfig != null) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public final int hashCode() {
        int result = name.hashCode();
        result = 31 * result + durability;
        result = 31 * result + capacity;
        result = 31 * result + poolSize;
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        result = 31 * result + (mergePolicyConfig != null ? mergePolicyConfig.hashCode() : 0);
        return result;
    }

    // non-private for testing
    static class ScheduledExecutorConfigReadOnly extends ScheduledExecutorConfig {

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

        @Override
        public ScheduledExecutorConfig setQuorumName(String quorumName) {
            throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
        }

        public ScheduledExecutorConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
            throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
        }
    }
}
