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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.eviction.EvictionConfiguration;
import com.hazelcast.internal.eviction.EvictionStrategyType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Configuration for eviction.
 * <p>
 * You can set a limit for number of
 * entries or total memory cost of entries.
 * <p>
 * The default values of the eviction configuration are:
 * <ul>
 * <li>{@link EvictionPolicy#LRU} as eviction policy</li>
 * <li>{@link MaxSizePolicy#ENTRY_COUNT} as max size policy</li>
 * <li>{@value MapConfig#DEFAULT_MAX_SIZE} as maximum
 * size for on-heap {@link com.hazelcast.map.IMap}</li>
 * <li>{@value DEFAULT_MAX_ENTRY_COUNT} as maximum size
 *      for all other data structures and configurations</li>
 * </ul>
 */
public class EvictionConfig implements EvictionConfiguration, IdentifiedDataSerializable, Serializable {

    /**
     * Default maximum entry count.
     */
    public static final int DEFAULT_MAX_ENTRY_COUNT = 10000;

    /**
     * Default Max-Size Policy.
     */
    public static final MaxSizePolicy DEFAULT_MAX_SIZE_POLICY = MaxSizePolicy.ENTRY_COUNT;

    /**
     * Default Eviction Policy.
     */
    public static final EvictionPolicy DEFAULT_EVICTION_POLICY = EvictionPolicy.LRU;

    protected int size = DEFAULT_MAX_ENTRY_COUNT;
    protected MaxSizePolicy maxSizePolicy = DEFAULT_MAX_SIZE_POLICY;
    protected EvictionPolicy evictionPolicy = DEFAULT_EVICTION_POLICY;

    protected String comparatorClassName;
    protected EvictionPolicyComparator comparator;

    /**
     * Used by the {@link NearCacheConfigAccessor} to
     * initialize the proper default value for on-heap maps.
     */
    boolean sizeConfigured;

    public EvictionConfig() {
    }

    public EvictionConfig(EvictionConfig config) {
        this.sizeConfigured = config.sizeConfigured;
        this.size = config.size;
        this.maxSizePolicy = config.maxSizePolicy;
        this.evictionPolicy = config.evictionPolicy;
        this.comparatorClassName = config.comparatorClassName;
        this.comparator = config.comparator;
    }

    /**
     * Returns the size which is used by the {@link MaxSizePolicy}.
     * <p>
     * The interpretation of the value depends
     * on the configured {@link MaxSizePolicy}.
     *
     * @return the size which is used by the {@link MaxSizePolicy}
     */
    public int getSize() {
        return size;
    }

    /**
     * Sets the size which is used by the {@link MaxSizePolicy}.
     * <p>
     * The interpretation of the value depends
     * on the configured {@link MaxSizePolicy}.
     * <p>
     * Accepts any non-negative number. The default
     * value is {@value #DEFAULT_MAX_ENTRY_COUNT}.
     *
     * @param size the size which is used by the {@link MaxSizePolicy}
     * @return this EvictionConfig instance
     */
    public EvictionConfig setSize(int size) {
        this.sizeConfigured = true;
        this.size = checkNotNegative(size,
                "size cannot be a negative number!");
        return this;
    }

    /**
     * Returns the {@link MaxSizePolicy} of this eviction configuration.
     *
     * @return the {@link MaxSizePolicy} of this eviction configuration
     */
    public MaxSizePolicy getMaxSizePolicy() {
        return maxSizePolicy;
    }

    /**
     * Sets the {@link MaxSizePolicy} of this eviction configuration.
     *
     * @param maxSizePolicy the {@link MaxSizePolicy} of this eviction configuration
     * @return this EvictionConfig instance
     */
    public EvictionConfig setMaxSizePolicy(MaxSizePolicy maxSizePolicy) {
        this.maxSizePolicy = checkNotNull(maxSizePolicy,
                "maxSizePolicy cannot be null!");
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
        this.evictionPolicy = checkNotNull(evictionPolicy,
                "Eviction policy cannot be null!");
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
     * Returns the class name of the configured {@link
     * EvictionPolicyComparator} implementation.
     *
     * @return the class name of the configured
     * {@link EvictionPolicyComparator} implementation
     */
    @Override
    public String getComparatorClassName() {
        return comparatorClassName;
    }

    /**
     * Sets the class name of the configured {@link
     * EvictionPolicyComparator} implementation.
     * <p>
     * Only one of the {@code comparator class name} and {@code
     * comparator} can be configured in the eviction configuration.
     *
     * @param comparatorClassName the class name of the
     *                            configured {@link EvictionPolicyComparator} implementation
     * @return this EvictionConfig instance
     */
    public EvictionConfig setComparatorClassName(@Nonnull String comparatorClassName) {
        this.comparatorClassName = checkHasText(comparatorClassName,
                "Eviction policy comparator class name cannot be null!");
        this.comparator = null;
        return this;
    }

    /**
     * Returns the instance of the configured {@link
     * EvictionPolicyComparator} implementation.
     *
     * @return the instance of the configured {@link
     * EvictionPolicyComparator} implementation
     */
    @Override
    public EvictionPolicyComparator getComparator() {
        return comparator;
    }

    /**
     * Sets the instance of the configured {@link
     * EvictionPolicyComparator} implementation.
     * <p>
     * Only one of the {@code comparator class name} and {@code
     * comparator} can be configured in the eviction configuration.
     *
     * @param comparator the instance of the configured
     *                   {@link EvictionPolicyComparator} implementation
     * @return this EvictionConfig instance
     * @see com.hazelcast.map.MapEvictionPolicyComparator
     * @see com.hazelcast.cache.CacheEvictionPolicyComparator
     */
    public EvictionConfig setComparator(@Nonnull EvictionPolicyComparator comparator) {
        this.comparator = checkNotNull(comparator,
                "Eviction policy comparator cannot be null!");
        this.comparatorClassName = null;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.EVICTION_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(size);
        out.writeString(maxSizePolicy.toString());
        out.writeString(evictionPolicy.toString());
        out.writeString(comparatorClassName);
        out.writeObject(comparator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        size = in.readInt();
        maxSizePolicy = MaxSizePolicy.valueOf(in.readString());
        evictionPolicy = EvictionPolicy.valueOf(in.readString());
        comparatorClassName = in.readString();
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
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EvictionConfig)) {
            return false;
        }

        EvictionConfig that = (EvictionConfig) o;

        return size == that.size
                && maxSizePolicy == that.maxSizePolicy
                && evictionPolicy == that.evictionPolicy
                && Objects.equals(comparatorClassName, that.comparatorClassName)
                && Objects.equals(comparator, that.comparator);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(size, maxSizePolicy, evictionPolicy, comparator, comparatorClassName);
    }
}
