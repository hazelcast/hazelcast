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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.ValidationUtil;

import java.io.IOException;
import java.io.Serializable;

/**
 * Contains configuration for an NearCache.
 */
public class NearCacheConfig
        implements DataSerializable , Serializable {

    /**
     * Default value of time to live in seconds.
     */
    public static final int DEFAULT_TTL_SECONDS = 0;
    /**
     * Default value of idle in seconds for eviction.
     */
    public static final int DEFAULT_MAX_IDLE_SECONDS = 0;
    /**
     * Default value of maximum size
     */
    public static final int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;
    /**
     * Default eviction policy
     */
    public static final String DEFAULT_EVICTION_POLICY = EvictionConfig.DEFAULT_EVICTION_POLICY.name();
    /**
     * Default memory format
     */
    public static final InMemoryFormat DEFAULT_MEMORY_FORMAT = InMemoryFormat.BINARY;

    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;

    private int maxSize = DEFAULT_MAX_SIZE;

    private String evictionPolicy = DEFAULT_EVICTION_POLICY;

    private int maxIdleSeconds = DEFAULT_MAX_IDLE_SECONDS;

    private boolean invalidateOnChange = true;

    private InMemoryFormat inMemoryFormat = DEFAULT_MEMORY_FORMAT;

    private String name = "default";

    private NearCacheConfigReadOnly readOnly;

    private boolean cacheLocalEntries;

    private LocalUpdatePolicy localUpdatePolicy = LocalUpdatePolicy.INVALIDATE;

    // Default value of eviction config is
    //      * ENTRY_COUNT with 10.000 max entry count
    //      * LRU as eviction policy
    private EvictionConfig evictionConfig = new EvictionConfig();

    /**
     * Local Update Policy enum.
     */
    public enum LocalUpdatePolicy {
        /**
         *INVALIDATE POLICY
         */
        INVALIDATE,
        /**
         *CACHE ON UPDATE POLICY
         */
        CACHE
    }

    public NearCacheConfig() {
    }

    public NearCacheConfig(String name) {
        this.name = name;
    }

    public NearCacheConfig(int timeToLiveSeconds, int maxSize, String evictionPolicy, int maxIdleSeconds,
                           boolean invalidateOnChange, InMemoryFormat inMemoryFormat) {
        this(timeToLiveSeconds, maxSize, evictionPolicy, maxIdleSeconds, invalidateOnChange, inMemoryFormat, null);
    }

    public NearCacheConfig(int timeToLiveSeconds, int maxSize, String evictionPolicy, int maxIdleSeconds,
                           boolean invalidateOnChange, InMemoryFormat inMemoryFormat, EvictionConfig evictionConfig) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxSize = maxSize;
        this.evictionPolicy = evictionPolicy;
        this.maxIdleSeconds = maxIdleSeconds;
        this.invalidateOnChange = invalidateOnChange;
        this.inMemoryFormat = inMemoryFormat;
        // Eviction config cannot be null
        if (evictionConfig != null) {
            this.evictionConfig = evictionConfig;
        }
    }

    public NearCacheConfig(NearCacheConfig config) {
        name = config.getName();
        evictionPolicy = config.getEvictionPolicy();
        inMemoryFormat = config.getInMemoryFormat();
        invalidateOnChange = config.isInvalidateOnChange();
        maxIdleSeconds = config.getMaxIdleSeconds();
        maxSize = config.getMaxSize();
        timeToLiveSeconds = config.getTimeToLiveSeconds();
        cacheLocalEntries = config.isCacheLocalEntries();
        localUpdatePolicy = config.localUpdatePolicy;
        // Eviction config cannot be null
        if (config.evictionConfig != null) {
            this.evictionConfig = config.evictionConfig;
        }
    }

    public NearCacheConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new NearCacheConfigReadOnly(this);
        }
        return readOnly;
    }

    public String getName() {
        return name;
    }

    public NearCacheConfig setName(String name) {
        this.name = name;
        return this;
    }

    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    public NearCacheConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        ValidationUtil.isNotNegative(timeToLiveSeconds, "TTL seconds cannot be negative !");

        this.timeToLiveSeconds = timeToLiveSeconds;
        return this;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public NearCacheConfig setMaxSize(int maxSize) {
        ValidationUtil.isNotNegative(maxSize, "Max-Size cannot be negative !");

        this.maxSize = maxSize;
        return this;
    }

    public String getEvictionPolicy() {
        return evictionPolicy;
    }

    public NearCacheConfig setEvictionPolicy(String evictionPolicy) {
        ValidationUtil.isNotNull(evictionPolicy, "Eviction policy cannot be null !");

        this.evictionPolicy = evictionPolicy;
        return this;
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public NearCacheConfig setMaxIdleSeconds(int maxIdleSeconds) {
        ValidationUtil.isNotNegative(maxIdleSeconds, "Max-Idle seconds cannot be negative !");

        this.maxIdleSeconds = maxIdleSeconds;
        return this;
    }

    public boolean isInvalidateOnChange() {
        return invalidateOnChange;
    }

    public NearCacheConfig setInvalidateOnChange(boolean invalidateOnChange) {
        this.invalidateOnChange = invalidateOnChange;
        return this;
    }

    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    public NearCacheConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        ValidationUtil.isNotNull(inMemoryFormat, "In-Memory format cannot be null !");

        this.inMemoryFormat = inMemoryFormat;
        return this;
    }

    public boolean isCacheLocalEntries() {
        return cacheLocalEntries;
    }

    public NearCacheConfig setCacheLocalEntries(boolean cacheLocalEntries) {
        this.cacheLocalEntries = cacheLocalEntries;
        return this;
    }

    public LocalUpdatePolicy getLocalUpdatePolicy() {
        return localUpdatePolicy;
    }

    public NearCacheConfig setLocalUpdatePolicy(LocalUpdatePolicy localUpdatePolicy) {
        ValidationUtil.isNotNull(localUpdatePolicy, "Local update policy cannot be null !");

        this.localUpdatePolicy = localUpdatePolicy;
        return this;
    }

    // this setter is for reflection based configuration building
    public NearCacheConfig setInMemoryFormat(String inMemoryFormat) {
        ValidationUtil.isNotNull(inMemoryFormat, "In-Memory format cannot be null !");

        this.inMemoryFormat = InMemoryFormat.valueOf(inMemoryFormat);
        return this;
    }

    public EvictionConfig getEvictionConfig() {
        return evictionConfig;
    }

    public NearCacheConfig setEvictionConfig(EvictionConfig evictionConfig) {
        ValidationUtil.isNotNull(evictionConfig, "Eviction config cannot be null !");

        this.evictionConfig = evictionConfig;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(evictionPolicy);
        out.writeInt(timeToLiveSeconds);
        out.writeInt(maxSize);
        out.writeBoolean(invalidateOnChange);
        out.writeBoolean(cacheLocalEntries);
        out.writeInt(inMemoryFormat.ordinal());
        out.writeInt(localUpdatePolicy.ordinal());
        out.writeObject(evictionConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        evictionPolicy = in.readUTF();
        timeToLiveSeconds = in.readInt();
        maxSize = in.readInt();
        invalidateOnChange = in.readBoolean();
        cacheLocalEntries = in.readBoolean();
        final int inMemoryFormatInt = in.readInt();
        inMemoryFormat = InMemoryFormat.values()[inMemoryFormatInt];
        final int localUpdatePolicyInt = in.readInt();
        localUpdatePolicy = LocalUpdatePolicy.values()[localUpdatePolicyInt];
        evictionConfig = in.readObject();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NearCacheConfig{");
        sb.append("timeToLiveSeconds=").append(timeToLiveSeconds);
        sb.append(", maxSize=").append(maxSize);
        sb.append(", evictionPolicy='").append(evictionPolicy).append('\'');
        sb.append(", maxIdleSeconds=").append(maxIdleSeconds);
        sb.append(", invalidateOnChange=").append(invalidateOnChange);
        sb.append(", inMemoryFormat=").append(inMemoryFormat);
        sb.append(", cacheLocalEntries=").append(cacheLocalEntries);
        sb.append(", localUpdatePolicy=").append(localUpdatePolicy);
        sb.append(", evictionConfig=").append(evictionConfig);
        sb.append('}');
        return sb.toString();
    }
}
