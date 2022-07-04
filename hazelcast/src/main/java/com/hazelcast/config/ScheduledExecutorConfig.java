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

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.ScheduledExecutorConfig.CapacityPolicy.PER_NODE;
import static com.hazelcast.config.ScheduledExecutorConfig.CapacityPolicy.getById;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Configuration options for the {@link IScheduledExecutorService}.
 */
public class ScheduledExecutorConfig implements IdentifiedDataSerializable, NamedConfig, Versioned {

    /**
     * The number of executor threads per Member for the Executor based on this configuration.
     */
    private static final int DEFAULT_POOL_SIZE = 16;

    /**
     * The number of tasks that can co-exist per scheduler according to the capacity policy.
     */
    private static final int DEFAULT_CAPACITY = 100;

    /**
     * The number of tasks that can co-exist per scheduler per partition.
     */
    private static final CapacityPolicy DEFAULT_CAPACITY_POLICY = PER_NODE;

    /**
     * The number of replicas per task scheduled in each ScheduledExecutor.
     */
    private static final int DEFAULT_DURABILITY = 1;

    private String name = "default";

    private int durability = DEFAULT_DURABILITY;

    private int capacity = DEFAULT_CAPACITY;

    private CapacityPolicy capacityPolicy = DEFAULT_CAPACITY_POLICY;

    private int poolSize = DEFAULT_POOL_SIZE;

    private String splitBrainProtectionName;

    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();

    private boolean statisticsEnabled = true;

    public ScheduledExecutorConfig() {
    }

    public ScheduledExecutorConfig(String name) {
        this.name = name;
    }

    public ScheduledExecutorConfig(String name, int durability, int capacity,
                                   int poolSize, boolean statisticsEnabled) {
        this(name, durability, capacity, poolSize, null,
                new MergePolicyConfig(), DEFAULT_CAPACITY_POLICY, statisticsEnabled);
    }

    public ScheduledExecutorConfig(String name, int durability, int capacity, int poolSize,
                                   String splitBrainProtectionName,
                                   MergePolicyConfig mergePolicyConfig,
                                   CapacityPolicy capacityPolicy,
                                   boolean statisticsEnabled) {
        this.name = name;
        this.durability = durability;
        this.poolSize = poolSize;
        this.capacity = capacity;
        this.capacityPolicy = capacityPolicy;
        this.splitBrainProtectionName = splitBrainProtectionName;
        this.mergePolicyConfig = mergePolicyConfig;
        this.statisticsEnabled = statisticsEnabled;
    }

    public ScheduledExecutorConfig(ScheduledExecutorConfig config) {
        this(config.getName(), config.getDurability(), config.getCapacity(),
                config.getPoolSize(), config.getSplitBrainProtectionName(),
                new MergePolicyConfig(config.getMergePolicyConfig()), config.getCapacityPolicy(),
                config.isStatisticsEnabled());
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
     * To prevent any undesirable data-loss, capacity is ignored during partition migrations,
     * the count is updated accordingly, however the rejection is not enforced.
     *
     * @param capacity the capacity of the executor
     * @return this executor config instance
     */
    public ScheduledExecutorConfig setCapacity(int capacity) {
        this.capacity = checkNotNegative(capacity, "capacity can't be smaller than 0");
        return this;
    }

    /**
     * @return the policy of the capacity setting
     */
    public CapacityPolicy getCapacityPolicy() {
        return capacityPolicy;
    }

    /**
     * Set the capacity policy for the configured capacity value
     *
     * To prevent any undesirable data-loss, capacity is ignored during partition migrations,
     * the count is updated accordingly, however the rejection is not enforced.
     *
     * @param capacityPolicy
     */
    public ScheduledExecutorConfig setCapacityPolicy(@Nonnull CapacityPolicy capacityPolicy) {
        checkNotNull(capacityPolicy);
        this.capacityPolicy = capacityPolicy;
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
    public ScheduledExecutorConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
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
    public ScheduledExecutorConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    @Override
    public String toString() {
        return "ScheduledExecutorConfig{"
                + "name='" + name + '\''
                + ", durability=" + durability
                + ", poolSize=" + poolSize
                + ", capacity=" + capacity
                + ", capacityPolicy=" + capacityPolicy
                + ", statisticsEnabled=" + statisticsEnabled
                + ", splitBrainProtectionName=" + splitBrainProtectionName
                + ", mergePolicyConfig=" + mergePolicyConfig
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.SCHEDULED_EXECUTOR_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(durability);
        out.writeInt(capacity);
        out.writeInt(poolSize);
        out.writeString(splitBrainProtectionName);
        out.writeObject(mergePolicyConfig);
        out.writeByte(capacityPolicy.getId());
        out.writeBoolean(statisticsEnabled);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        durability = in.readInt();
        capacity = in.readInt();
        poolSize = in.readInt();
        splitBrainProtectionName = in.readString();
        mergePolicyConfig = in.readObject();
        capacityPolicy = getById(in.readByte());
        statisticsEnabled = in.readBoolean();
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
        if (capacityPolicy != that.capacityPolicy) {
            return false;
        }
        if (poolSize != that.poolSize) {
            return false;
        }
        if (!Objects.equals(splitBrainProtectionName, that.splitBrainProtectionName)) {
            return false;
        }
        if (!Objects.equals(mergePolicyConfig, that.mergePolicyConfig)) {
            return false;
        }
        if (statisticsEnabled != that.statisticsEnabled) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public final int hashCode() {
        int result = name.hashCode();
        result = 31 * result + durability;
        result = 31 * result + capacity;
        result = 31 * result + capacityPolicy.hashCode();
        result = 31 * result + poolSize;
        result = 31 * result + (splitBrainProtectionName != null ? splitBrainProtectionName.hashCode() : 0);
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        result = 31 * result + (mergePolicyConfig != null ? mergePolicyConfig.hashCode() : 0);
        return result;
    }

    /**
     * Capacity policy options
     */
    public enum CapacityPolicy {

        /**
         * Capacity policy that counts tasks per Hazelcast instance node/member,
         * and rejects new ones when {@link #capacity} value is reached
         */
        PER_NODE((byte) 0),

        /**
         * Capacity policy that counts tasks per partition, and rejects new ones when {@link #capacity} value is reached.
         * Storage size depends on the partition count in a Hazelcast instance.
         * This policy should not be used often.
         * Avoid using this policy with a small cluster: if the cluster is small it will
         * be hosting more partitions, and therefore tasks, than that of a larger cluster.
         *
         * This policy has no effect when scheduling is done using the OnMember APIs
         * eg. {@link IScheduledExecutorService#scheduleOnMember(Runnable, Member, long, TimeUnit)}
         */
        PER_PARTITION((byte) 1);

        /**
         * Unique identifier for a policy, can be used for de/serialization purposes
         */
        private final byte id;

        CapacityPolicy(byte id) {
            this.id = id;
        }

        public byte getId() {
            return id;
        }

        public static CapacityPolicy getById(final byte id) {
            for (CapacityPolicy policy : values()) {
                if (policy.id == id) {
                    return policy;
                }
            }

            return null;
        }
    }
}
