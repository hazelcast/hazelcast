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

import com.hazelcast.internal.eviction.EvictionConfiguration;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.internal.eviction.EvictionPolicyType;
import com.hazelcast.internal.eviction.EvictionStrategyType;
import com.hazelcast.internal.eviction.impl.EvictionConfigHelper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Configuration for eviction.
 * You can set a limit for number of entries or total memory cost of entries.
 */
public class EvictionConfig
        implements EvictionConfiguration, DataSerializable, Serializable {

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

    protected EvictionConfig readOnly;

    public EvictionConfig() {
    }

    public EvictionConfig(int size, MaxSizePolicy maxSizePolicy, EvictionPolicy evictionPolicy) {
        /**
         * ===== NOTE =====
         *
         * Do not use setters, because they are overridden in the readonly version of this config and
         * they cause an "UnsupportedOperationException". Just set directly if the value is valid.
         */

        // Size cannot be non-positive number
        this.size = checkPositive(size, "Size must be positive number!");
        // Max-Size policy cannot be null
        this.maxSizePolicy = checkNotNull(maxSizePolicy, "Max-Size policy cannot be null!");
        // Eviction policy cannot be null
        this.evictionPolicy = checkNotNull(evictionPolicy, "Eviction policy cannot be null!");
    }

    public EvictionConfig(int size, MaxSizePolicy maxSizePolicy, String comparatorClassName) {
        /**
         * ===== NOTE =====
         *
         * Do not use setters, because they are overridden in the readonly version of this config and
         * they cause an "UnsupportedOperationException". Just set directly if the value is valid.
         */

        // Size cannot be non-positive number
        this.size = checkPositive(size, "Size must be positive number!");
        // Max-Size policy cannot be null
        this.maxSizePolicy = checkNotNull(maxSizePolicy, "Max-Size policy cannot be null!");
        // Eviction policy comparator class name cannot be null
        this.comparatorClassName = checkNotNull(comparatorClassName, "Comparator classname cannot be null!");
    }

    public EvictionConfig(int size, MaxSizePolicy maxSizePolicy, EvictionPolicyComparator comparator) {
        /**
         * ===== NOTE =====
         *
         * Do not use setters, because they are overridden in the readonly version of this config and
         * they cause an "UnsupportedOperationException". Just set directly if the value is valid.
         */

        // Size cannot be non-positive number
        this.size = checkPositive(size, "Size must be positive number!");
        // Max-Size policy cannot be null
        this.maxSizePolicy = checkNotNull(maxSizePolicy, "Max-Size policy cannot be null!");
        // Eviction policy comparator cannot be null
        this.comparator = checkNotNull(comparator, "Comparator cannot be null!");
    }

    public EvictionConfig(EvictionConfig config) {
        /**
         * ===== NOTE =====
         *
         * Do not use setters, because they are overriden in readonly version of this config and
         * cause "UnsupportedOperationException". So just set directly if value is valid.
         */

        // Size cannot be non-positive number
        this.size = checkPositive(config.size, "Size must be positive number!");
        // Max-Size policy cannot be null
        this.maxSizePolicy = checkNotNull(config.maxSizePolicy, "Max-Size policy cannot be null!");
        // Eviction policy cannot be null
        if (config.evictionPolicy != null) {
            this.evictionPolicy = config.evictionPolicy;
        }
        // Eviction policy comparator class name cannot be null
        if (config.comparatorClassName != null) {
            this.comparatorClassName = config.comparatorClassName;
        }
        // Eviction policy comparator cannot be null
        if (config.comparator != null) {
            this.comparator = config.comparator;
        }
        EvictionConfigHelper.checkEvictionConfig(evictionPolicy, comparatorClassName, comparator);
    }

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

    public EvictionConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new EvictionConfigReadOnly(this);
        }
        return readOnly;
    }

    public int getSize() {
        return size;
    }

    public EvictionConfig setSize(int size) {
        this.size = checkPositive(size, "Size must be positive number !");
        return this;
    }

    public MaxSizePolicy getMaximumSizePolicy() {
        return maxSizePolicy;
    }

    public EvictionConfig setMaximumSizePolicy(MaxSizePolicy maxSizePolicy) {
        this.maxSizePolicy = checkNotNull(maxSizePolicy, "Max-Size policy cannot be null !");
        return this;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    public EvictionConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
        checkNotNull(evictionPolicy, "Eviction policy cannot be null !");

        this.evictionPolicy = evictionPolicy;
        return this;
    }

    @Override
    public String getComparatorClassName() {
        return comparatorClassName;
    }

    public EvictionConfig setComparatorClassName(String comparatorClassName) {
        checkNotNull(comparatorClassName, "Eviction policy comparator class name cannot be null !");

        this.comparatorClassName = comparatorClassName;
        return this;
    }

    @Override
    public EvictionPolicyComparator getComparator() {
        return comparator;
    }

    public EvictionConfig setComparator(EvictionPolicyComparator comparator) {
        checkNotNull(comparator, "Eviction policy comparator cannot be null !");

        this.comparator = comparator;
        return this;
    }

    @Override
    public EvictionStrategyType getEvictionStrategyType() {
        // TODO Add support for other/custom eviction strategies
        return EvictionStrategyType.DEFAULT_EVICTION_STRATEGY;
    }

    @Override
    public EvictionPolicyType getEvictionPolicyType() {
        if (evictionPolicy == EvictionPolicy.LFU) {
            return EvictionPolicyType.LFU;
        } else if (evictionPolicy == EvictionPolicy.LRU) {
            return EvictionPolicyType.LRU;
        } else {
            return null;
        }
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeInt(size);
        out.writeUTF(maxSizePolicy.toString());
        out.writeUTF(evictionPolicy.toString());
        out.writeUTF(comparatorClassName);
        out.writeObject(comparator);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
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
                + ", readOnly=" + readOnly
                + '}';
    }

}
