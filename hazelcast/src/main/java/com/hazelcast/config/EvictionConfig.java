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

import com.hazelcast.internal.eviction.EvictionConfiguration;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.internal.eviction.EvictionPolicyType;
import com.hazelcast.internal.eviction.EvictionStrategyType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.IOException;
import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Configuration for eviction.
 * You can set a limit for number of entries or total memory cost of entries.
 * <p>
 * The default values of the eviction configuration are
 * <ul>
 * <li>{@link EvictionPolicy#LRU} as eviction policy</li>
 * <li>{@link EvictionConfig.MaxSizePolicy#ENTRY_COUNT} as max size policy</li>
 * <li>{@value DEFAULT_MAX_ENTRY_COUNT_FOR_ON_HEAP_MAP} as maximum size for on-heap {@link com.hazelcast.core.IMap}</li>
 * <li>{@value DEFAULT_MAX_ENTRY_COUNT} as maximum size for all other data structures and configurations</li>
 * </ul>
 */
@BinaryInterface
public class EvictionConfig implements EvictionConfiguration, DataSerializable, Serializable {

    /**
     * Default maximum entry count.
     */
    public static final int DEFAULT_MAX_ENTRY_COUNT = 10000;

    /**
     * Default maximum entry count for Map on-heap Near Caches.
     */
    public static final int DEFAULT_MAX_ENTRY_COUNT_FOR_ON_HEAP_MAP = Integer.MAX_VALUE;

    /**
     * Default Max-Size Policy.
     */
    public static final MaxSizePolicy DEFAULT_MAX_SIZE_POLICY = MaxSizePolicy.ENTRY_COUNT;

    /**
     * Default Eviction Policy.
     */
    public static final EvictionPolicy DEFAULT_EVICTION_POLICY = EvictionPolicy.LRU;

    /**
     * Maximum Size Policy
     */
    public enum MaxSizePolicy {
        /**
         * Policy based on maximum number of entries stored per data structure (map, cache etc)
         */
        ENTRY_COUNT,
        /**
         * Policy based on maximum used native memory in megabytes per data structure (map, cache etc)
         * on each Hazelcast instance
         */
        USED_NATIVE_MEMORY_SIZE,
        /**
         * Policy based on maximum used native memory percentage per data structure (map, cache etc)
         * on each Hazelcast instance
         */
        USED_NATIVE_MEMORY_PERCENTAGE,
        /**
         * Policy based on minimum free native memory in megabytes per Hazelcast instance
         */
        FREE_NATIVE_MEMORY_SIZE,
        /**
         * Policy based on minimum free native memory percentage per Hazelcast instance
         */
        FREE_NATIVE_MEMORY_PERCENTAGE
    }

    protected int size = DEFAULT_MAX_ENTRY_COUNT;
    protected MaxSizePolicy maxSizePolicy = DEFAULT_MAX_SIZE_POLICY;
    protected EvictionPolicy evictionPolicy = DEFAULT_EVICTION_POLICY;
    protected String comparatorClassName;
    protected EvictionPolicyComparator comparator;

    protected EvictionConfig readOnly;

    /**
     * Used by the {@link NearCacheConfigAccessor} to initialize the proper default value for on-heap maps.
     */
    boolean sizeConfigured;

    public EvictionConfig() {
    }

    public EvictionConfig(int size, MaxSizePolicy maxSizePolicy, EvictionPolicy evictionPolicy) {
        this.sizeConfigured = true;
        this.size = checkPositive(size, "Size must be positive number!");
        this.maxSizePolicy = checkNotNull(maxSizePolicy, "Max-Size policy cannot be null!");
        this.evictionPolicy = checkNotNull(evictionPolicy, "Eviction policy cannot be null!");
    }

    public EvictionConfig(int size, MaxSizePolicy maxSizePolicy, String comparatorClassName) {
        this.sizeConfigured = true;
        this.size = checkPositive(size, "Size must be positive number!");
        this.maxSizePolicy = checkNotNull(maxSizePolicy, "Max-Size policy cannot be null!");
        this.comparatorClassName = checkNotNull(comparatorClassName, "Comparator classname cannot be null!");
    }

    public EvictionConfig(int size, MaxSizePolicy maxSizePolicy, EvictionPolicyComparator comparator) {
        this.sizeConfigured = true;
        this.size = checkPositive(size, "Size must be positive number!");
        this.maxSizePolicy = checkNotNull(maxSizePolicy, "Max-Size policy cannot be null!");
        this.comparator = checkNotNull(comparator, "Comparator cannot be null!");
    }

    public EvictionConfig(EvictionConfig config) {
        this.sizeConfigured = true;
        this.size = checkPositive(config.size, "Size must be positive number!");
        this.maxSizePolicy = checkNotNull(config.maxSizePolicy, "Max-Size policy cannot be null!");
        if (config.evictionPolicy != null) {
            this.evictionPolicy = config.evictionPolicy;
        }
        // Eviction policy comparator class name is not allowed to be null
        if (config.comparatorClassName != null) {
            this.comparatorClassName = config.comparatorClassName;
        }
        // Eviction policy comparator is not allowed to be null
        if (config.comparator != null) {
            this.comparator = config.comparator;
        }
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public EvictionConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new EvictionConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Returns the size which is used by the {@link MaxSizePolicy}.
     * <p>
     * The interpretation of the value depends on the configured {@link MaxSizePolicy}.
     *
     * @return the size which is used by the {@link MaxSizePolicy}
     */
    public int getSize() {
        return size;
    }

    /**
     * Sets the size which is used by the {@link MaxSizePolicy}.
     * <p>
     * The interpretation of the value depends on the configured {@link MaxSizePolicy}.
     * <p>
     * Accepts any positive number. The default value is {@value #DEFAULT_MAX_ENTRY_COUNT}.
     *
     * @param size the size which is used by the {@link MaxSizePolicy}
     * @return this EvictionConfig instance
     */
    public EvictionConfig setSize(int size) {
        this.sizeConfigured = true;
        this.size = checkPositive(size, "size must be positive number!");
        return this;
    }

    /**
     * Returns the {@link MaxSizePolicy} of this eviction configuration.
     *
     * @return the {@link MaxSizePolicy} of this eviction configuration
     */
    public MaxSizePolicy getMaximumSizePolicy() {
        return maxSizePolicy;
    }

    /**
     * Sets the {@link MaxSizePolicy} of this eviction configuration.
     *
     * @param maxSizePolicy the {@link MaxSizePolicy} of this eviction configuration
     * @return this EvictionConfig instance
     */
    public EvictionConfig setMaximumSizePolicy(MaxSizePolicy maxSizePolicy) {
        this.maxSizePolicy = checkNotNull(maxSizePolicy, "maxSizePolicy cannot be null!");
        return this;
    }

    /**
     * Returns the {@link EvictionPolicy} of this eviction configuration.
     *
     * @return the {@link EvictionPolicy} of this eviction configuration
     */
    @Override
    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    /**
     * Sets the {@link EvictionPolicy} of this eviction configuration.
     *
     * @param evictionPolicy the {@link EvictionPolicy} of this eviction configuration
     * @return this EvictionConfig instance
     */
    public EvictionConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
        this.evictionPolicy = checkNotNull(evictionPolicy, "Eviction policy cannot be null!");
        return this;
    }

    /**
     * Returns the {@link EvictionStrategyType} of this eviction configuration.
     *
     * @return the {@link EvictionStrategyType} of this eviction configuration
     */
    @Override
    public EvictionStrategyType getEvictionStrategyType() {
        return EvictionStrategyType.DEFAULT_EVICTION_STRATEGY;
    }

    /**
     * Returns the {@link EvictionPolicyType} of this eviction configuration.
     *
     * @return the {@link EvictionPolicyType} of this eviction configuration
     * @deprecated since 3.9, please use {@link #getEvictionPolicy()}
     */
    @Deprecated
    public EvictionPolicyType getEvictionPolicyType() {
        switch (evictionPolicy) {
            case LFU:
                return EvictionPolicyType.LFU;
            case LRU:
                return EvictionPolicyType.LRU;
            case RANDOM:
                return EvictionPolicyType.RANDOM;
            case NONE:
                return EvictionPolicyType.NONE;
            default:
                return null;
        }
    }

    /**
     * Returns the class name of the configured {@link EvictionPolicyComparator} implementation.
     *
     * @return the class name of the configured {@link EvictionPolicyComparator} implementation
     */
    @Override
    public String getComparatorClassName() {
        return comparatorClassName;
    }

    /**
     * Sets the class name of the configured {@link EvictionPolicyComparator} implementation.
     * <p>
     * Only one of the {@code comparator class name} and {@code comparator} can be configured in the eviction configuration.
     *
     * @param comparatorClassName the class name of the configured {@link EvictionPolicyComparator} implementation
     * @return this EvictionConfig instance
     */
    public EvictionConfig setComparatorClassName(String comparatorClassName) {
        this.comparatorClassName = checkNotNull(comparatorClassName, "Eviction policy comparator class name cannot be null!");
        return this;
    }

    /**
     * Returns the instance of the configured {@link EvictionPolicyComparator} implementation.
     *
     * @return the instance of the configured {@link EvictionPolicyComparator} implementation
     */
    @Override
    public EvictionPolicyComparator getComparator() {
        return comparator;
    }

    /**
     * Sets the instance of the configured {@link EvictionPolicyComparator} implementation.
     * <p>
     * Only one of the {@code comparator class name} and {@code comparator} can be configured in the eviction configuration.
     *
     * @param comparator the instance of the configured {@link EvictionPolicyComparator} implementation
     * @return this EvictionConfig instance
     */
    public EvictionConfig setComparator(EvictionPolicyComparator comparator) {
        this.comparator = checkNotNull(comparator, "Eviction policy comparator cannot be null!");
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(size);
        out.writeUTF(maxSizePolicy.toString());
        out.writeUTF(evictionPolicy.toString());
        out.writeUTF(comparatorClassName);
        out.writeObject(comparator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        size = in.readInt();
        maxSizePolicy = MaxSizePolicy.valueOf(in.readUTF());
        evictionPolicy = EvictionPolicy.valueOf(in.readUTF());
        comparatorClassName = in.readUTF();
        comparator = in.readObject();
    }

    @Override
    public String toString() {
        return "EvictionConfig{"
                + "size=" + size
                + ", maxSizePolicy=" + maxSizePolicy
                + ", evictionPolicy=" + evictionPolicy
                + ", comparatorClassName=" + comparatorClassName
                + ", comparator=" + comparator
                + '}';
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EvictionConfig)) {
            return false;
        }

        EvictionConfig that = (EvictionConfig) o;

        if (size != that.size) {
            return false;
        }
        if (maxSizePolicy != that.maxSizePolicy) {
            return false;
        }
        if (evictionPolicy != that.evictionPolicy) {
            return false;
        }
        if (comparatorClassName != null
                ? !comparatorClassName.equals(that.comparatorClassName) : that.comparatorClassName != null) {
            return false;
        }
        return comparator != null ? comparator.equals(that.comparator) : that.comparator == null;
    }

    @Override
    public final int hashCode() {
        int result = size;
        result = 31 * result + (maxSizePolicy != null ? maxSizePolicy.hashCode() : 0);
        result = 31 * result + (evictionPolicy != null ? evictionPolicy.hashCode() : 0);
        result = 31 * result + (comparatorClassName != null ? comparatorClassName.hashCode() : 0);
        result = 31 * result + (comparator != null ? comparator.hashCode() : 0);
        return result;
    }
}
