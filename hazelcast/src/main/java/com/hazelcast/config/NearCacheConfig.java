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

import java.io.IOException;
import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains configuration for an NearCache.
 */
public class NearCacheConfig
        implements DataSerializable, Serializable {

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
         * INVALIDATE POLICY
         */
        INVALIDATE,
        /**
         * CACHE ON UPDATE POLICY
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

    /**
     * Gets the name of the near cache.
     *
     * @return The name of the near cache.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the near cache.
     *
     * @param name The name of the near cache.
     * @return This near cache config instance.
     */
    public NearCacheConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the maximum number of seconds for each entry to stay in the near cache. Entries that are
     * older than time-to-live-seconds will get automatically evicted from the near cache.
     *
     * @return The maximum number of seconds for each entry to stay in the near cache.
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * Sets the maximum number of seconds for each entry to stay in the near cache. Entries that are
     * older than time-to-live-seconds will get automatically evicted from the near cache.
     * Any integer between 0 and Integer.MAX_VALUE. 0 means infinite. Default is 0.
     *
     * @param timeToLiveSeconds The maximum number of seconds for each entry to stay in the near cache.
     * @return This near cache config instance.
     */
    public NearCacheConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = checkNotNegative(timeToLiveSeconds, "TTL seconds cannot be negative !");
        return this;
    }

    /**
    * Gets the maximum size of the near cache. When max size is reached,
    * cache is evicted based on the policy defined.
     *
     * @return The maximum size of the near cache.
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * Sets the maximum size of the near cache. When max size is reached,
     * cache is evicted based on the policy defined.
     * Any integer between 0 and Integer.MAX_VALUE. 0 means
     * Integer.MAX_VALUE. Default is 0.
     *
     * @param maxSize The maximum number of seconds for each entry to stay in the near cache.
     * @return This near cache config instance.
     */
    public NearCacheConfig setMaxSize(int maxSize) {
        this.maxSize = checkNotNegative(maxSize, "Max-Size cannot be negative !");
        return this;
    }

    /**
     * The eviction policy for the near cache.
     * Valid values are:
     * NONE (no extra eviction, time-to-live-seconds may still apply),
     * LRU  (Least Recently Used),
     * LFU  (Least Frequently Used).
     * NONE is the default.
     * Regardless of the eviction policy used, time-to-live-seconds will still apply.
     *
     * @return TThe eviction policy for the near cache.
     */
    public String getEvictionPolicy() {
        return evictionPolicy;
    }

    /**
     * Valid values are:
     * NONE (no extra eviction, time-to-live-seconds may still apply),
     * LRU  (Least Recently Used),
     * LFU  (Least Frequently Used).
     * NONE is the default.
     * Regardless of the eviction policy used, time-to-live-seconds will still apply.
     *
     * @param evictionPolicy The eviction policy for the near cache.
     * @return This near cache config instance.
     */
    public NearCacheConfig setEvictionPolicy(String evictionPolicy) {
        this.evictionPolicy = checkNotNull(evictionPolicy, "Eviction policy cannot be null !");
        return this;
    }

    /**
     * Maximum number of seconds each entry can stay in the near cache as untouched (not-read).
     * Entries that are not read (touched) more than max-idle-seconds value will get removed
     * from the near cache.
     *
     * @return Maximum number of seconds each entry can stay in the near cache as 
     *         untouched (not-read).
     */
    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    /**
     * Maximum number of seconds each entry can stay in the near cache as untouched (not-read).
     * Entries that are not read (touched) more than max-idle-seconds value will get removed
     * from the near cache.
     * Any integer between 0 and Integer.MAX_VALUE. 0 means Integer.MAX_VALUE. Default is 0.
     *
     * @param maxIdleSeconds Maximum number of seconds each entry can stay in the near cache as 
     *        untouched (not-read).
     * @return This near cache config instance.
     */
    public NearCacheConfig setMaxIdleSeconds(int maxIdleSeconds) {
        this.maxIdleSeconds = checkNotNegative(maxIdleSeconds, "Max-Idle seconds cannot be negative !");
        return this;
    }

    /**
     * True to evict the cached entries if the entries are changed (updated or removed).
     *
     * @return If true, the cached entries are evicted if the entires are 
     * changed (updated or removed), false otherwise.
     * @return This near cache config instance.
     */
    public boolean isInvalidateOnChange() {
        return invalidateOnChange;
    }

    /**
     * True to evict the cached entries if the entries are changed (updated or removed).
     *
     * @param invalidateOnChange True to evict the cached entries if the entries are 
     * changed (updated or removed), false otherwise.
     * @return This near cache config instance.
     */
    public NearCacheConfig setInvalidateOnChange(boolean invalidateOnChange) {
        this.invalidateOnChange = invalidateOnChange;
        return this;
    }

    /**
     * Gets the data type used to store entries.
     * Possible values:
     * BINARY (default): keys and values are stored as binary data.
     * OBJECT: values are stored in their object forms.
     * NATIVE: keys and values are stored in native memory.
     *
     * @return The data type used to store entries.
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Sets the data type used to store entries.
     * Possible values:
     * BINARY (default): keys and values are stored as binary data.
     * OBJECT: values are stored in their object forms.
     * NATIVE: keys and values are stored in native memory.
     *
     * @param inMemoryFormat The data type used to store entries.
     * @return This near cache config instance.
     */
    public NearCacheConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat, "In-Memory format cannot be null !");
        return this;
    }

    /**
     * If true, cache local entries also.
     * This is useful when in-memory-format for near cache is different than the map's one.
     *
     * @return True if local entries are cached also.
     */
    public boolean isCacheLocalEntries() {
        return cacheLocalEntries;
    }

    /**
     * True to cache local entries also.
     * This is useful when in-memory-format for near cache is different than the map's one.
     *
     * @param cacheLocalEntries True to cache local entries also.
     * @return This near cache config instance.
     */
    public NearCacheConfig setCacheLocalEntries(boolean cacheLocalEntries) {
        this.cacheLocalEntries = cacheLocalEntries;
        return this;
    }

    public LocalUpdatePolicy getLocalUpdatePolicy() {
        return localUpdatePolicy;
    }

    public NearCacheConfig setLocalUpdatePolicy(LocalUpdatePolicy localUpdatePolicy) {
        this.localUpdatePolicy = checkNotNull(localUpdatePolicy, "Local update policy cannot be null !");
        return this;
    }

    // this setter is for reflection based configuration building
    public NearCacheConfig setInMemoryFormat(String inMemoryFormat) {
        checkNotNull(inMemoryFormat, "In-Memory format cannot be null !");

        this.inMemoryFormat = InMemoryFormat.valueOf(inMemoryFormat);
        return this;
    }

    /**
     * The eviction policy.
     * NONE (no extra eviction, time-to-live-seconds may still apply),
     * LRU  (Least Recently Used),
     * LFU  (Least Frequently Used).
     * Regardless of the eviction policy used, time-to-live-seconds will still apply.
     *
     * @return The eviction policy.
     */
    public EvictionConfig getEvictionConfig() {
        return evictionConfig;
    }

    /**
     * The eviction policy.
     * Valid values are:
     * NONE (no extra eviction, time-to-live-seconds may still apply),
     * LRU  (Least Recently Used),
     * LFU  (Least Frequently Used).
     * Regardless of the eviction policy used, time-to-live-seconds will still apply.
     *
     * @param evictionConfig The eviction policy.
     * @return This near cache config instance.
     */
    public NearCacheConfig setEvictionConfig(EvictionConfig evictionConfig) {
        this.evictionConfig = checkNotNull(evictionConfig, "Eviction config cannot be null !");
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
