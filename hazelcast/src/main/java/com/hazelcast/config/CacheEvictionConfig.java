/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.eviction.impl.policies.LFUEvictionPolicyStrategyFactory;
import com.hazelcast.cache.impl.eviction.impl.policies.LRUEvictionPolicyStrategyFactory;
import com.hazelcast.cache.impl.eviction.impl.sampling.SamplingBasedEvictionStrategyFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Configuration for cache's capacity.
 * You can set a limit for number of entries or total memory cost of entries.
 */
public class CacheEvictionConfig
        implements DataSerializable, Serializable {

    /**
     * Default maximum size of cache.
     */
    public static final int DEFAULT_MAX_SIZE = 10000;

    /**
     * Default factory instance for the sampling based {@link com.hazelcast.cache.impl.eviction.EvictionStrategy}.
     * This is also the default implementation if eviction is not deactivated.
     */
    public static final String DEFAULT_EVICTION_STRATEGY_FACTORY = SamplingBasedEvictionStrategyFactory.class.getCanonicalName();

    /**
     * Default factory instance of <tt>LRU</tt> (Less Recently Used) eviction policy. The bundled LRU implementation
     * is stateless and therefore thread-safe by design. Because of this the stateless implementation, the factory always
     * returns the same instance of the policy implementation.
     */
    public static final String LRU_EVICTION_POLICY_STRATEGY_FACTORY = LRUEvictionPolicyStrategyFactory.class.getCanonicalName();

    /**
     * Default factory instance of <tt>LFU</tt> (Less Frequently Used) eviction policy. The bundled LFU implementation
     * is stateless and therefore thread-safe by design. Because of this the stateless implementation, the factory always
     * returns the same instance of the policy implementation.
     */
    public static final String LFU_EVICTION_POLICY_STRATEGY_FACTORY = LFUEvictionPolicyStrategyFactory.class.getCanonicalName();

    /**
     * The factory for the default {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy} which is the
     * sampling based implementation {@link com.hazelcast.cache.impl.eviction.impl.sampling.SamplingBasedEvictionStrategy}
     */
    public static final String DEFAULT_EVICTION_POLICY_STRATEGY_FACTORY = LRU_EVICTION_POLICY_STRATEGY_FACTORY;

    private CacheEvictionConfigReadOnly readOnly;

    private CacheMaxSizePolicy maxSizePolicy = CacheMaxSizePolicy.ENTRY_COUNT;

    private int size = DEFAULT_MAX_SIZE;

    private String evictionStrategyFactory = DEFAULT_EVICTION_STRATEGY_FACTORY;

    private String evictionPolicyStrategyFactory = DEFAULT_EVICTION_POLICY_STRATEGY_FACTORY;

    public CacheEvictionConfig() {
    }

    public CacheEvictionConfig(int size, CacheMaxSizePolicy maxSizePolicy) {
        setSize(size);
        this.maxSizePolicy = maxSizePolicy;
    }

    public CacheEvictionConfig(CacheEvictionConfig config) {
        this.size = config.size;
        this.maxSizePolicy = config.maxSizePolicy;
    }

    /**
     * Maximum Size Policy
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
        FREE_NATIVE_MEMORY_PERCENTAGE
    }

    public CacheEvictionConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new CacheEvictionConfigReadOnly(this);
        }
        return readOnly;
    }

    public int getSize() {
        return size;
    }

    public CacheEvictionConfig setSize(int size) {
        if (size > 0) {
            this.size = size;
        }
        return this;
    }

    public CacheMaxSizePolicy getMaxSizePolicy() {
        return maxSizePolicy;
    }

    public CacheEvictionConfig setMaxSizePolicy(CacheMaxSizePolicy maxSizePolicy) {
        this.maxSizePolicy = maxSizePolicy;
        return this;
    }

    public String getEvictionStrategyFactory() {
        return evictionStrategyFactory;
    }

    public CacheEvictionConfig setEvictionStrategyFactory(String evictionStrategyFactory) {
        this.evictionStrategyFactory = evictionStrategyFactory;
        return this;
    }

    public String getEvictionPolicyStrategyFactory() {
        return evictionPolicyStrategyFactory;
    }

    public CacheEvictionConfig setEvictionPolicyStrategyFactory(String evictionPolicyStrategyFactory) {
        this.evictionPolicyStrategyFactory = evictionPolicyStrategyFactory;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(maxSizePolicy.toString());
        out.writeInt(size);
        out.writeUTF(evictionStrategyFactory);
        out.writeUTF(evictionPolicyStrategyFactory);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        maxSizePolicy = CacheMaxSizePolicy.valueOf(in.readUTF());
        size = in.readInt();
        evictionStrategyFactory = in.readUTF();
        evictionPolicyStrategyFactory = in.readUTF();
    }
}
