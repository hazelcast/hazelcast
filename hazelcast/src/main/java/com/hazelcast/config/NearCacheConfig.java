/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.io.Serializable;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains the configuration for a Near Cache.
 */
public class NearCacheConfig implements IdentifiedDataSerializable, Serializable {

    /**
     * Default value of the time to live in seconds.
     */
    public static final int DEFAULT_TTL_SECONDS = 0;

    /**
     * Default value of the idle time for eviction in seconds.
     */
    public static final int DEFAULT_MAX_IDLE_SECONDS = 0;

    /**
     * Default value for the in-memory format.
     */
    public static final InMemoryFormat DEFAULT_MEMORY_FORMAT = InMemoryFormat.BINARY;

    /**
     * Default value of the maximum size.
     *
     * @deprecated since 3.8, please use {@link EvictionConfig#DEFAULT_MAX_ENTRY_COUNT_FOR_ON_HEAP_MAP}
     */
    public static final int DEFAULT_MAX_SIZE = EvictionConfig.DEFAULT_MAX_ENTRY_COUNT_FOR_ON_HEAP_MAP;

    /**
     * Default value for the eviction policy.
     *
     * @deprecated since 3.8, please use {@link EvictionConfig#DEFAULT_EVICTION_POLICY}
     */
    public static final String DEFAULT_EVICTION_POLICY = EvictionConfig.DEFAULT_EVICTION_POLICY.name();

    /**
     * Used to decide how to reflect local updates to near cache.
     */
    public enum LocalUpdatePolicy {
        /**
         * Local put and local remove immediately invalidates near cache
         */
        INVALIDATE,

        /**
         * Local put immediately adds new value to near cache. Local remove works as in INVALIDATE mode
         */
        CACHE_ON_UPDATE,

        /**
         * Subject to remove in further releases. Instead of this use {@link LocalUpdatePolicy#CACHE_ON_UPDATE}
         */
        @Deprecated
        CACHE
    }

    private boolean cacheLocalEntries;
    private boolean invalidateOnChange = true;
    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;
    private int maxIdleSeconds = DEFAULT_MAX_IDLE_SECONDS;
    private int maxSize = EvictionConfig.DEFAULT_MAX_ENTRY_COUNT_FOR_ON_HEAP_MAP;
    private String name = "default";
    private String evictionPolicy = EvictionConfig.DEFAULT_EVICTION_POLICY.name();
    private InMemoryFormat inMemoryFormat = DEFAULT_MEMORY_FORMAT;
    private LocalUpdatePolicy localUpdatePolicy = LocalUpdatePolicy.INVALIDATE;
    private NearCacheConfigReadOnly readOnly;

    /**
     * Default value of eviction config is
     * <ul>
     * <li>ENTRY_COUNT as max size policy</li>
     * <li>10000 as maximum size</li>
     * <li>LRU as eviction policy</li>
     * </ul>
     */
    private EvictionConfig evictionConfig = new EvictionConfig();
    private NearCachePreloaderConfig preloaderConfig = new NearCachePreloaderConfig();

    public NearCacheConfig() {
    }

    public NearCacheConfig(String name) {
        this.name = name;
    }

    public NearCacheConfig(int timeToLiveSeconds, int maxIdleSeconds, boolean invalidateOnChange,
                           InMemoryFormat inMemoryFormat) {
        this(timeToLiveSeconds, maxIdleSeconds, invalidateOnChange, inMemoryFormat, null);
    }

    public NearCacheConfig(int timeToLiveSeconds, int maxIdleSeconds, boolean invalidateOnChange,
                           InMemoryFormat inMemoryFormat, EvictionConfig evictionConfig) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxSize = calculateMaxSize(maxSize);
        this.maxIdleSeconds = maxIdleSeconds;
        this.invalidateOnChange = invalidateOnChange;
        this.inMemoryFormat = inMemoryFormat;
        // EvictionConfig is not allowed to be null
        if (evictionConfig != null) {
            this.evictionConfig = evictionConfig;
            this.evictionPolicy = evictionConfig.getEvictionPolicy().toString();
            this.maxSize = evictionConfig.getSize();
        }
    }

    /**
     * @deprecated since 3.8,
     * please use {@link NearCacheConfig#NearCacheConfig(int, int, boolean, InMemoryFormat)}
     */
    public NearCacheConfig(int timeToLiveSeconds, int maxSize, String evictionPolicy, int maxIdleSeconds,
                           boolean invalidateOnChange, InMemoryFormat inMemoryFormat) {
        this(timeToLiveSeconds, maxSize, evictionPolicy, maxIdleSeconds, invalidateOnChange, inMemoryFormat, null);
    }

    /**
     * @deprecated since 3.8,
     * please use {@link NearCacheConfig#NearCacheConfig(int, int, boolean, InMemoryFormat, EvictionConfig)}
     */
    public NearCacheConfig(int timeToLiveSeconds, int maxSize, String evictionPolicy, int maxIdleSeconds,
                           boolean invalidateOnChange, InMemoryFormat inMemoryFormat, EvictionConfig evictionConfig) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxSize = calculateMaxSize(maxSize);
        this.evictionPolicy = evictionPolicy;
        this.maxIdleSeconds = maxIdleSeconds;
        this.invalidateOnChange = invalidateOnChange;
        this.inMemoryFormat = inMemoryFormat;
        // EvictionConfig is not allowed to be null
        if (evictionConfig != null) {
            this.evictionConfig = evictionConfig;
        } else {
            this.evictionConfig.setSize(calculateMaxSize(maxSize));
            this.evictionConfig.setEvictionPolicy(EvictionPolicy.valueOf(evictionPolicy));
            this.evictionConfig.setMaximumSizePolicy(ENTRY_COUNT);
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
        // EvictionConfig is not allowed to be null
        if (config.evictionConfig != null) {
            this.evictionConfig = config.evictionConfig;
        }
        // NearCachePreloaderConfig is not allowed to be null
        if (config.preloaderConfig != null) {
            this.preloaderConfig = config.preloaderConfig;
        }
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return Immutable version of this configuration.
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only.
     */
    public NearCacheConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new NearCacheConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of the Near Cache.
     *
     * @return The name of the Near Cache.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the Near Cache.
     *
     * @param name The name of the Near Cache.
     * @return This Near Cache config instance.
     */
    public NearCacheConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the maximum number of seconds for each entry to stay in the Near Cache. Entries that are
     * older than time-to-live-seconds will get automatically evicted from the Near Cache.
     *
     * @return The maximum number of seconds for each entry to stay in the Near Cache.
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * Sets the maximum number of seconds for each entry to stay in the Near Cache. Entries that are
     * older than time-to-live-seconds will get automatically evicted from the Near Cache.
     * Any integer between 0 and Integer.MAX_VALUE. 0 means infinite. Default is 0.
     *
     * @param timeToLiveSeconds The maximum number of seconds for each entry to stay in the Near Cache.
     * @return This Near Cache config instance.
     */
    public NearCacheConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = checkNotNegative(timeToLiveSeconds, "TTL seconds cannot be negative!");
        return this;
    }

    /**
     * Gets the maximum size of the Near Cache. When max size is reached,
     * cache is evicted based on the policy defined.
     *
     * @return The maximum size of the Near Cache.
     * @deprecated since 3.8, use {@link #getEvictionConfig()} and {@link EvictionConfig#getSize()} instead
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * Sets the maximum size of the Near Cache. When max size is reached,
     * cache is evicted based on the policy defined.
     * Any integer between 0 and Integer.MAX_VALUE. 0 means
     * Integer.MAX_VALUE. Default is 0.
     *
     * @param maxSize The maximum number of seconds for each entry to stay in the Near Cache.
     * @return This Near Cache config instance.
     * @deprecated since 3.8, use {@link #setEvictionConfig(EvictionConfig)} and {@link EvictionConfig#setSize(int)} instead
     */
    public NearCacheConfig setMaxSize(int maxSize) {
        checkNotNegative(maxSize, "maxSize cannot be a negative number!");
        this.maxSize = calculateMaxSize(maxSize);
        this.evictionConfig.setSize(this.maxSize);
        this.evictionConfig.setMaximumSizePolicy(ENTRY_COUNT);
        return this;
    }

    /**
     * Returns the eviction policy for the Near Cache.
     *
     * @return The eviction policy for the Near Cache.
     * @deprecated since 3.8, use {@link #getEvictionConfig()} and {@link EvictionConfig#getEvictionPolicy()} instead
     */
    public String getEvictionPolicy() {
        return evictionPolicy;
    }

    /**
     * Sets the eviction policy.
     *
     * Valid values are:
     * LRU  (Least Recently Used)
     * LFU  (Least Frequently Used)
     * NONE (no extra eviction, time-to-live-seconds or max-idle-seconds may still apply)
     * RANDOM (random entry)
     *
     * LRU is the default.
     * Regardless of the eviction policy used, time-to-live-seconds and max-idle-seconds will still apply.
     *
     * @param evictionPolicy The eviction policy for the Near Cache.
     * @return This Near Cache config instance.
     * @deprecated since 3.8, use {@link #setEvictionConfig(EvictionConfig)}
     * and {@link EvictionConfig#setEvictionPolicy(EvictionPolicy)} instead
     */
    public NearCacheConfig setEvictionPolicy(String evictionPolicy) {
        this.evictionPolicy = checkNotNull(evictionPolicy, "Eviction policy cannot be null!");
        this.evictionConfig.setEvictionPolicy(EvictionPolicy.valueOf(evictionPolicy));
        this.evictionConfig.setMaximumSizePolicy(ENTRY_COUNT);
        return this;
    }

    /**
     * Maximum number of seconds each entry can stay in the Near Cache as untouched (not-read).
     * Entries that are not read (touched) more than max-idle-seconds value will get removed
     * from the Near Cache.
     *
     * @return Maximum number of seconds each entry can stay in the Near Cache as
     * untouched (not-read).
     */
    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    /**
     * Maximum number of seconds each entry can stay in the Near Cache as untouched (not-read).
     * Entries that are not read (touched) more than max-idle-seconds value will get removed
     * from the Near Cache.
     * Any integer between 0 and Integer.MAX_VALUE. 0 means Integer.MAX_VALUE. Default is 0.
     *
     * @param maxIdleSeconds Maximum number of seconds each entry can stay in the Near Cache as
     *                       untouched (not-read).
     * @return This Near Cache config instance.
     */
    public NearCacheConfig setMaxIdleSeconds(int maxIdleSeconds) {
        this.maxIdleSeconds = checkNotNegative(maxIdleSeconds, "Max-Idle seconds cannot be negative!");
        return this;
    }

    /**
     * True to evict the cached entries if the entries are changed (updated or removed).
     *
     * When true, the member listens for cluster-wide changes on the entries and invalidates
     * them on change. Changes done on the local member always invalidate the cache.
     *
     * @return This Near Cache config instance.
     */
    public boolean isInvalidateOnChange() {
        return invalidateOnChange;
    }

    /**
     * True to evict the cached entries if the entries are changed (updated or removed).
     *
     * If set to true, the member will listen for cluster-wide changes on the entries and invalidate
     * them on change. Changes done on the local member always invalidate the cache.
     *
     * @param invalidateOnChange True to evict the cached entries if the entries are
     *                           changed (updated or removed), false otherwise.
     * @return This Near Cache config instance.
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
     * @return This Near Cache config instance.
     */
    public NearCacheConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat, "In-Memory format cannot be null!");
        return this;
    }

    /**
     * If true, cache local entries also.
     * This is useful when in-memory-format for Near Cache is different than the map's one.
     *
     * @return True if local entries are cached also.
     */
    public boolean isCacheLocalEntries() {
        return cacheLocalEntries;
    }

    /**
     * True to cache local entries also.
     * This is useful when in-memory-format for Near Cache is different than the map's one.
     *
     * @param cacheLocalEntries True to cache local entries also.
     * @return This Near Cache config instance.
     */
    public NearCacheConfig setCacheLocalEntries(boolean cacheLocalEntries) {
        this.cacheLocalEntries = cacheLocalEntries;
        return this;
    }

    public LocalUpdatePolicy getLocalUpdatePolicy() {
        return localUpdatePolicy;
    }

    public NearCacheConfig setLocalUpdatePolicy(LocalUpdatePolicy localUpdatePolicy) {
        this.localUpdatePolicy = checkNotNull(localUpdatePolicy, "Local update policy cannot be null!");
        return this;
    }

    // this setter is for reflection based configuration building
    public NearCacheConfig setInMemoryFormat(String inMemoryFormat) {
        checkNotNull(inMemoryFormat, "In-Memory format cannot be null!");

        this.inMemoryFormat = InMemoryFormat.valueOf(inMemoryFormat);
        return this;
    }

    /**
     * The eviction configuration.
     *
     * @return The eviction configuration.
     */
    public EvictionConfig getEvictionConfig() {
        return evictionConfig;
    }

    /**
     * Sets the eviction configuration.
     *
     * @param evictionConfig The eviction configuration.
     * @return This Near Cache config instance.
     */
    public NearCacheConfig setEvictionConfig(EvictionConfig evictionConfig) {
        this.evictionConfig = checkNotNull(evictionConfig, "EvictionConfig cannot be null!");
        return this;
    }

    public NearCachePreloaderConfig getPreloaderConfig() {
        return preloaderConfig;
    }

    public NearCacheConfig setPreloaderConfig(NearCachePreloaderConfig preloaderConfig) {
        this.preloaderConfig = checkNotNull(preloaderConfig, "NearCachePreloaderConfig cannot be null!");
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.NEAR_CACHE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(evictionPolicy);
        out.writeInt(timeToLiveSeconds);
        out.writeInt(maxIdleSeconds);
        out.writeInt(maxSize);
        out.writeBoolean(invalidateOnChange);
        out.writeBoolean(cacheLocalEntries);
        out.writeInt(inMemoryFormat.ordinal());
        out.writeInt(localUpdatePolicy.ordinal());
        out.writeObject(evictionConfig);
        out.writeObject(preloaderConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        evictionPolicy = in.readUTF();
        timeToLiveSeconds = in.readInt();
        maxIdleSeconds = in.readInt();
        maxSize = in.readInt();
        invalidateOnChange = in.readBoolean();
        cacheLocalEntries = in.readBoolean();
        inMemoryFormat = InMemoryFormat.values()[in.readInt()];
        localUpdatePolicy = LocalUpdatePolicy.values()[in.readInt()];
        evictionConfig = in.readObject();
        preloaderConfig = in.readObject();
    }

    @Override
    public String toString() {
        return "NearCacheConfig{"
                + "name=" + name
                + ", timeToLiveSeconds=" + timeToLiveSeconds
                + ", maxSize=" + maxSize
                + ", evictionPolicy='" + evictionPolicy + '\''
                + ", maxIdleSeconds=" + maxIdleSeconds
                + ", invalidateOnChange=" + invalidateOnChange
                + ", inMemoryFormat=" + inMemoryFormat
                + ", cacheLocalEntries=" + cacheLocalEntries
                + ", localUpdatePolicy=" + localUpdatePolicy
                + ", evictionConfig=" + evictionConfig
                + ", preloaderConfig=" + preloaderConfig
                + '}';
    }

    private int calculateMaxSize(int maxSize) {
        return (maxSize == 0) ? Integer.MAX_VALUE : checkNotNegative(maxSize, "Max-size cannot be negative!");
    }
}
