/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.eviction.EvictionConfiguration;
import com.hazelcast.cache.impl.eviction.EvictionPolicyType;
import com.hazelcast.cache.impl.eviction.EvictionStrategyType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkFalse;
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
     * Default Eviction Policy.
     */
    public static final EvictionPolicy DEFAULT_EVICTION_POLICY = EvictionPolicy.LRU;

    private int size = DEFAULT_MAX_ENTRY_COUNT;
    private MaxSizePolicy maxSizePolicy = MaxSizePolicy.ENTRY_COUNT;
    private EvictionPolicy evictionPolicy = DEFAULT_EVICTION_POLICY;

    private EvictionConfigReadOnly readOnly;

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
        if (size > 0) {
            this.size = size;
        }
        // Max-Size policy cannot be null
        if (maxSizePolicy != null) {
            this.maxSizePolicy = maxSizePolicy;
        }
        // Eviction policy cannot be null or NONE
        if (evictionPolicy != null && evictionPolicy != EvictionPolicy.NONE) {
            this.evictionPolicy = evictionPolicy;
        }
    }

    public EvictionConfig(EvictionConfig config) {
        /**
         * ===== NOTE =====
         *
         * Do not use setters, because they are overriden in readonly version of this config and
         * cause "UnsupportedOperationException". So just set directly if value is valid.
         */

        // Size cannot be non-positive number
        if (config.size > 0) {
            this.size = config.size;
        }
        // Max-Size policy cannot be null
        if (config.maxSizePolicy != null) {
            this.maxSizePolicy = config.maxSizePolicy;
        }
        // Eviction policy cannot be null or NONE
        if (config.evictionPolicy != null && config.evictionPolicy != EvictionPolicy.NONE) {
            this.evictionPolicy = config.evictionPolicy;
        }
    }

    /**
     * Maximum Size Policy
     */
    public enum MaxSizePolicy {
        /**
         * Decide maximum entry count according to node
         */
        ENTRY_COUNT,
        /**
         * Decide maximum size with use native memory size
         */
        USED_NATIVE_MEMORY_SIZE,
        /**
         * Decide maximum size with use native memory percentage
         */
        USED_NATIVE_MEMORY_PERCENTAGE,
        /**
         * Decide minimum free native memory size to trigger cleanup
         */
        FREE_NATIVE_MEMORY_SIZE,
        /**
         * Decide minimum free native memory percentage to trigger cleanup
         */
        FREE_NATIVE_MEMORY_PERCENTAGE
    }

    public EvictionConfigReadOnly getAsReadOnly() {
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

    public MaxSizePolicy getMaxSizePolicy() {
        return maxSizePolicy;
    }

    public EvictionConfig setMaxSizePolicy(MaxSizePolicy maxSizePolicy) {
        this.maxSizePolicy = checkNotNull(maxSizePolicy, "Max-Size policy cannot be null !");
        return this;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    public EvictionConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
        checkNotNull(evictionPolicy, "Eviction policy cannot be null !");
        checkFalse(EvictionPolicy.NONE == evictionPolicy, "Eviction policy cannot be \"NONE\" !");

        this.evictionPolicy = evictionPolicy;
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
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        size = in.readInt();
        maxSizePolicy = MaxSizePolicy.valueOf(in.readUTF());
        evictionPolicy = EvictionPolicy.valueOf(in.readUTF());
    }

    @Override
    public String toString() {
        return "EvictionConfig{"
                + "size=" + size
                + ", maxSizePolicy=" + maxSizePolicy
                + ", evictionPolicy=" + evictionPolicy
                + ", readOnly=" + readOnly
                + '}';
    }

}
