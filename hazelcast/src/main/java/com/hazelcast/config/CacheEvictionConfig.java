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

import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.nio.serialization.BinaryInterface;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Configuration for cache eviction.
 *
 * @see com.hazelcast.config.EvictionConfig
 * @deprecated Use {@link com.hazelcast.config.EvictionConfig} instead of this
 */
@Deprecated
@BinaryInterface
public class CacheEvictionConfig extends EvictionConfig {

    public CacheEvictionConfig() {
    }

    public CacheEvictionConfig(int size, MaxSizePolicy maxSizePolicy, EvictionPolicy evictionPolicy) {
        super(size, maxSizePolicy, evictionPolicy);
    }

    public CacheEvictionConfig(int size, CacheMaxSizePolicy cacheMaxSizePolicy, EvictionPolicy evictionPolicy) {
        super(size, checkNotNull(cacheMaxSizePolicy,
                "Cache max-size policy cannot be null!").toMaxSizePolicy(), evictionPolicy);
    }

    public CacheEvictionConfig(int size, MaxSizePolicy maxSizePolicy, String comparatorClassName) {
        super(size, maxSizePolicy, comparatorClassName);
    }

    public CacheEvictionConfig(int size, CacheMaxSizePolicy cacheMaxSizePolicy, String comparatorClassName) {
        super(size, checkNotNull(cacheMaxSizePolicy,
                "Cache max-size policy cannot be null!").toMaxSizePolicy(), comparatorClassName);
    }

    public CacheEvictionConfig(int size, MaxSizePolicy maxSizePolicy, EvictionPolicyComparator comparator) {
        super(size, maxSizePolicy, comparator);
    }

    public CacheEvictionConfig(int size, CacheMaxSizePolicy cacheMaxSizePolicy, EvictionPolicyComparator comparator) {
        super(size, checkNotNull(cacheMaxSizePolicy,
                "Cache max-size policy cannot be null!").toMaxSizePolicy(), comparator);
    }

    public CacheEvictionConfig(EvictionConfig config) {
        super(config);
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    @Override
    public CacheEvictionConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new CacheEvictionConfigReadOnly(this);
        }
        return (CacheEvictionConfig) readOnly;
    }

    /**
     * Cache Maximum Size Policy
     */
    public enum CacheMaxSizePolicy {
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
        FREE_NATIVE_MEMORY_PERCENTAGE;

        public MaxSizePolicy toMaxSizePolicy() {
            switch (this) {
                case ENTRY_COUNT:
                    return MaxSizePolicy.ENTRY_COUNT;
                case USED_NATIVE_MEMORY_SIZE:
                    return MaxSizePolicy.USED_NATIVE_MEMORY_SIZE;
                case USED_NATIVE_MEMORY_PERCENTAGE:
                    return MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
                case FREE_NATIVE_MEMORY_SIZE:
                    return MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE;
                case FREE_NATIVE_MEMORY_PERCENTAGE:
                    return MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
                default:
                    throw new IllegalArgumentException("Invalid Cache Max-Size policy for converting to MaxSizePolicy");
            }
        }

        public static CacheMaxSizePolicy fromMaxSizePolicy(MaxSizePolicy maxSizePolicy) {
            switch (maxSizePolicy) {
                case ENTRY_COUNT:
                    return CacheMaxSizePolicy.ENTRY_COUNT;
                case USED_NATIVE_MEMORY_SIZE:
                    return CacheMaxSizePolicy.USED_NATIVE_MEMORY_SIZE;
                case USED_NATIVE_MEMORY_PERCENTAGE:
                    return CacheMaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
                case FREE_NATIVE_MEMORY_SIZE:
                    return CacheMaxSizePolicy.FREE_NATIVE_MEMORY_SIZE;
                case FREE_NATIVE_MEMORY_PERCENTAGE:
                    return CacheMaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
                default:
                    throw new IllegalArgumentException("Invalid Max-Size policy for converting to CacheMaxSizePolicy");
            }
        }
    }

    /**
     * Gets the {@link com.hazelcast.config.EvictionConfig.MaxSizePolicy} as
     * {@link com.hazelcast.config.CacheEvictionConfig.CacheMaxSizePolicy}.
     *
     * @return the {@link com.hazelcast.config.EvictionConfig.MaxSizePolicy} as
     * {@link com.hazelcast.config.CacheEvictionConfig.CacheMaxSizePolicy}
     * @deprecated Use {@link com.hazelcast.config.EvictionConfig#getMaximumSizePolicy()} instead of this
     */
    public CacheMaxSizePolicy getMaxSizePolicy() {
        return CacheMaxSizePolicy.fromMaxSizePolicy(getMaximumSizePolicy());
    }

    /**
     * Sets the {@link com.hazelcast.config.EvictionConfig.MaxSizePolicy} by using specified
     * {@link com.hazelcast.config.CacheEvictionConfig.CacheMaxSizePolicy}.
     *
     * @param cacheMaxSizePolicy {@link com.hazelcast.config.CacheEvictionConfig.CacheMaxSizePolicy} to be converted
     *                           and set as {@link com.hazelcast.config.EvictionConfig.MaxSizePolicy}
     * @return this {@link com.hazelcast.config.CacheEvictionConfig}
     * @deprecated Use {@link com.hazelcast.config.EvictionConfig#setMaximumSizePolicy(MaxSizePolicy)} instead of this
     */
    public CacheEvictionConfig setMaxSizePolicy(CacheMaxSizePolicy cacheMaxSizePolicy) {
        checkNotNull(cacheMaxSizePolicy, "Cache Max-Size policy cannot be null!");
        setMaximumSizePolicy(cacheMaxSizePolicy.toMaxSizePolicy());
        return this;
    }

    @Override
    public CacheEvictionConfig setMaximumSizePolicy(MaxSizePolicy maxSizePolicy) {
        super.setMaximumSizePolicy(maxSizePolicy);
        return this;
    }

    @Override
    public CacheEvictionConfig setSize(int size) {
        super.setSize(size);
        return this;
    }

    @Override
    public CacheEvictionConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
        super.setEvictionPolicy(evictionPolicy);
        return this;
    }

    @Override
    public CacheEvictionConfig setComparatorClassName(String comparatorClassName) {
        super.setComparatorClassName(comparatorClassName);
        return this;
    }

    @Override
    public CacheEvictionConfig setComparator(EvictionPolicyComparator comparator) {
        super.setComparator(comparator);
        return this;
    }

    @Override
    public String toString() {
        return "CacheEvictionConfig{"
                + "size=" + size
                + ", maxSizePolicy=" + maxSizePolicy
                + ", evictionPolicy=" + evictionPolicy
                + ", comparatorClassName=" + comparatorClassName
                + ", comparator=" + comparator
                + '}';
    }
}
