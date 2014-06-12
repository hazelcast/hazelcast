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

public class NearCacheConfig {
    public static final int DEFAULT_TTL_SECONDS = 0;
    public static final int DEFAULT_MAX_IDLE_SECONDS = 0;
    public static final int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;
    public static final String DEFAULT_EVICTION_POLICY = "LRU";
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

    public NearCacheConfig() {
    }

    public NearCacheConfig(int timeToLiveSeconds, int maxSize, String evictionPolicy
            , int maxIdleSeconds, boolean invalidateOnChange, InMemoryFormat inMemoryFormat) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxSize = maxSize;
        this.evictionPolicy = evictionPolicy;
        this.maxIdleSeconds = maxIdleSeconds;
        this.invalidateOnChange = invalidateOnChange;
        this.inMemoryFormat = inMemoryFormat;
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

    public void setName(String name) {
        this.name = name;
    }

    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    public NearCacheConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        return this;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public NearCacheConfig setMaxSize(int maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    public String getEvictionPolicy() {
        return evictionPolicy;
    }

    public NearCacheConfig setEvictionPolicy(String evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        return this;
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public NearCacheConfig setMaxIdleSeconds(int maxIdleSeconds) {
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

    // this setter is for reflection based configuration building
    public NearCacheConfig setInMemoryFormat(String inMemoryFormat) {
        this.inMemoryFormat = InMemoryFormat.valueOf(inMemoryFormat);
        return this;
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
        sb.append('}');
        return sb.toString();
    }
}
