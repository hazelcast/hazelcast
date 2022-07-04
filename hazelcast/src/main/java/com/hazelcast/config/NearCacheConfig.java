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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Contains the configuration for a Near Cache.
 */
@SuppressWarnings("checkstyle:methodcount")
public class NearCacheConfig implements IdentifiedDataSerializable, Serializable, NamedConfig {

    /**
     * Default value for the in-memory format.
     */
    public static final InMemoryFormat DEFAULT_MEMORY_FORMAT = InMemoryFormat.BINARY;

    /**
     * Do not serialize by default
     */
    public static final boolean DEFAULT_SERIALIZE_KEYS = false;

    /**
     * @see LocalUpdatePolicy#INVALIDATE
     */
    public static final boolean DEFAULT_INVALIDATE_ON_CHANGE = true;

    /**
     * @see LocalUpdatePolicy#INVALIDATE
     */
    public static final LocalUpdatePolicy DEFAULT_LOCAL_UPDATE_POLICY = LocalUpdatePolicy.INVALIDATE;

    /**
     * Default value of the time to live in seconds.
     */
    public static final int DEFAULT_TTL_SECONDS = 0;

    /**
     * Default value of the maximum idle time for eviction in seconds.
     */
    public static final int DEFAULT_MAX_IDLE_SECONDS = 0;

    /**
     * Default name when it is not set explicitly.
     */
    public static final String DEFAULT_NAME = "default";

    /**
     * Defines how to reflect local updates to the Near Cache.
     */
    public enum LocalUpdatePolicy {
        /**
         * Local put and local remove
         * immediately invalidate Near Cache.
         */
        INVALIDATE,

        /**
         * While local remove immediately invalidates
         * Near Cache, local put adds new value to it.
         */
        CACHE_ON_UPDATE
    }

    private boolean cacheLocalEntries;
    private boolean serializeKeys = DEFAULT_SERIALIZE_KEYS;
    private boolean invalidateOnChange = DEFAULT_INVALIDATE_ON_CHANGE;
    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;
    private int maxIdleSeconds = DEFAULT_MAX_IDLE_SECONDS;
    private String name = DEFAULT_NAME;
    private EvictionConfig evictionConfig = new EvictionConfig();
    private InMemoryFormat inMemoryFormat = DEFAULT_MEMORY_FORMAT;
    private LocalUpdatePolicy localUpdatePolicy = DEFAULT_LOCAL_UPDATE_POLICY;
    private NearCachePreloaderConfig preloaderConfig = new NearCachePreloaderConfig();

    public NearCacheConfig() {
    }

    public NearCacheConfig(String name) {
        setName(name);
    }

    public NearCacheConfig(NearCacheConfig config) {
        this.name = config.name;
        this.inMemoryFormat = config.inMemoryFormat;
        this.serializeKeys = config.serializeKeys;
        this.invalidateOnChange = config.invalidateOnChange;
        this.timeToLiveSeconds = config.timeToLiveSeconds;
        this.maxIdleSeconds = config.maxIdleSeconds;
        this.evictionConfig = config.evictionConfig;
        this.cacheLocalEntries = config.cacheLocalEntries;
        this.localUpdatePolicy = config.localUpdatePolicy;
        this.preloaderConfig = config.preloaderConfig;
    }

    /**
     * Returns the name of the Near Cache.
     *
     * @return the name of the Near Cache
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the Near Cache.
     *
     * @param name the name of the Near Cache
     * @return this Near Cache config instance
     */
    public NearCacheConfig setName(String name) {
        this.name = isNotNull(name, "name");
        return this;
    }

    /**
     * Returns the data type used to store entries.
     * <p>
     * Possible values:
     * <ul>
     * <li>{@code BINARY}: keys and values are stored as binary data</li>
     * <li>{@code OBJECT}: values are stored in their object forms</li>
     * <li>{@code NATIVE}: keys and values are stored in native memory</li>
     * </ul>
     * The default value is {@code BINARY}.
     *
     * @return the data type used to store entries
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Sets the data type used to store entries.
     * <p>
     * Possible values:
     * <ul>
     * <li>{@code BINARY}: keys and values are stored as binary data</li>
     * <li>{@code OBJECT}: values are stored in their object forms</li>
     * <li>{@code NATIVE}: keys and values are stored in native memory</li>
     * </ul>
     * The default value is {@code BINARY}.
     *
     * @param inMemoryFormat the data type used to store entries
     * @return this Near Cache config instance
     */
    public NearCacheConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat, "inMemoryFormat");
        return this;
    }

    // this setter is for reflection based configuration building
    public NearCacheConfig setInMemoryFormat(String inMemoryFormat) {
        isNotNull(inMemoryFormat, "In-Memory format cannot be null!");

        this.inMemoryFormat = InMemoryFormat.valueOf(inMemoryFormat);
        return this;
    }

    /**
     * Checks if the Near Cache key is stored in serialized format or by-reference.
     *
     * <b>NOTE:</b> When the in-memory-format is {@code NATIVE}, this method will always return {@code true}.
     *
     * @return {@code true} if the key is stored in serialized format or in-memory-format is {@code NATIVE},
     * {@code false} if the key is stored by-reference and in-memory-format is {@code BINARY} or {@code OBJECT}
     */
    public boolean isSerializeKeys() {
        return serializeKeys || inMemoryFormat == InMemoryFormat.NATIVE;
    }

    /**
     * Sets if the Near Cache key is stored in serialized format or by-reference.
     * <p>
     * <b>NOTE:</b> It's not supported to disable the key serialization when the in-memory-format is {@code NATIVE}.
     * You can still set this value to {@code false}, but it will have no effect.
     *
     * @param serializeKeys {@code true} if the key is stored in serialized format, {@code false} if stored by-reference
     * @return this Near Cache config instance
     */
    public NearCacheConfig setSerializeKeys(boolean serializeKeys) {
        this.serializeKeys = serializeKeys;
        return this;
    }

    /**
     * Checks if Near Cache entries are invalidated when the entries in the backing data structure are changed
     * (updated or removed).
     * <p>
     * When this setting is enabled, a Hazelcast instance with a Near Cache listens for cluster-wide changes
     * on the entries of the backing data structure and invalidates its corresponding Near Cache entries.
     * Changes done on the local Hazelcast instance always invalidate the Near Cache immediately.
     *
     * @return {@code true} if Near Cache invalidations are enabled on changes, {@code false} otherwise
     */
    public boolean isInvalidateOnChange() {
        return invalidateOnChange;
    }

    /**
     * Sets if Near Cache entries are invalidated when the entries in the backing data structure are changed
     * (updated or removed).
     * <p>
     * When this setting is enabled, a Hazelcast instance with a Near Cache listens for cluster-wide changes
     * on the entries of the backing data structure and invalidates its corresponding Near Cache entries.
     * Changes done on the local Hazelcast instance always invalidate the Near Cache immediately.
     *
     * @param invalidateOnChange {@code true} to enable Near Cache invalidations, {@code false} otherwise
     * @return this Near Cache config instance
     */
    public NearCacheConfig setInvalidateOnChange(boolean invalidateOnChange) {
        this.invalidateOnChange = invalidateOnChange;
        return this;
    }

    /**
     * Returns the maximum number of seconds for each entry to stay in the Near Cache (time to live).
     * <p>
     * Entries that are older than {@code timeToLiveSeconds} will automatically be evicted from the Near Cache.
     *
     * @return the maximum number of seconds for each entry to stay in the Near Cache
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * Returns the maximum number of seconds for each entry to stay in the Near Cache (time to live).
     * <p>
     * Entries that are older than {@code timeToLiveSeconds} will automatically be evicted from the Near Cache.
     * <p>
     * Accepts any integer between {@code 0} and {@link Integer#MAX_VALUE}.
     * The value {@code 0} means {@link Integer#MAX_VALUE}.
     * The default is {@code 0}.
     *
     * @param timeToLiveSeconds the maximum number of seconds for each entry to stay in the Near Cache
     * @return this Near Cache config instance
     */
    public NearCacheConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = checkNotNegative(timeToLiveSeconds, "TTL seconds cannot be negative!");
        return this;
    }

    /**
     * Returns the maximum number of seconds each entry can stay in the Near Cache as untouched (not-read).
     * <p>
     * Entries that are not read (touched) more than {@code maxIdleSeconds} value will get removed from the Near Cache.
     *
     * @return maximum number of seconds each entry can stay in the Near Cache as untouched (not-read)
     */
    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    /**
     * Set the maximum number of seconds each entry can stay in the Near Cache as untouched (not-read).
     * <p>
     * Entries that are not read (touched) more than {@code maxIdleSeconds} value will get removed from the Near Cache.
     * <p>
     * Accepts any integer between {@code 0} and {@link Integer#MAX_VALUE}.
     * The value {@code 0} means {@link Integer#MAX_VALUE}.
     * The default is {@code 0}.
     *
     * @param maxIdleSeconds maximum number of seconds each entry can stay in the Near Cache as untouched (not-read)
     * @return this Near Cache config instance
     */
    public NearCacheConfig setMaxIdleSeconds(int maxIdleSeconds) {
        this.maxIdleSeconds = checkNotNegative(maxIdleSeconds, "Max-Idle seconds cannot be negative!");
        return this;
    }

    /**
     * Returns the eviction configuration for this Near Cache.
     *
     * @return the eviction configuration
     */
    public EvictionConfig getEvictionConfig() {
        return evictionConfig;
    }

    /**
     * Sets the eviction configuration for this Near Cache.
     *
     * @param evictionConfig the eviction configuration
     * @return this Near Cache config instance
     */
    public NearCacheConfig setEvictionConfig(EvictionConfig evictionConfig) {
        this.evictionConfig = checkNotNull(evictionConfig, "EvictionConfig cannot be null!");
        return this;
    }

    /**
     * Checks if local entries are also cached in the Near Cache.
     * <p>
     * This is useful when the in-memory format of the Near Cache is different from the backing data structure.
     * This setting has no meaning on Hazelcast clients, since they have no local entries.
     *
     * @return {@code true} if local entries are also cached, {@code false} otherwise
     */
    public boolean isCacheLocalEntries() {
        return cacheLocalEntries;
    }

    /**
     * Sets if local entries are also cached in the Near Cache.
     * <p>
     * This is useful when the in-memory format of the Near Cache is different from the backing data structure.
     * This setting has no meaning on Hazelcast clients, since they have no local entries.
     *
     * @param cacheLocalEntries {@code true} if local entries are also cached, {@code false} otherwise
     * @return this Near Cache config instance
     */
    public NearCacheConfig setCacheLocalEntries(boolean cacheLocalEntries) {
        this.cacheLocalEntries = cacheLocalEntries;
        return this;
    }

    /**
     * Returns the {@link LocalUpdatePolicy} of this Near Cache.
     *
     * @return the {@link LocalUpdatePolicy} of this Near Cache
     */
    public LocalUpdatePolicy getLocalUpdatePolicy() {
        return localUpdatePolicy;
    }

    /**
     * Sets the {@link LocalUpdatePolicy} of this Near Cache.
     * <p>
     * This is only implemented for {@code JCache} data structures.
     *
     * @param localUpdatePolicy the {@link LocalUpdatePolicy} of this Near Cache
     * @return this Near Cache config instance
     */
    public NearCacheConfig setLocalUpdatePolicy(LocalUpdatePolicy localUpdatePolicy) {
        this.localUpdatePolicy = checkNotNull(localUpdatePolicy, "Local update policy cannot be null!");
        return this;
    }

    /**
     * Returns the {@link NearCachePreloaderConfig} of this Near Cache.
     *
     * @return the {@link NearCachePreloaderConfig} of this Near Cache
     */
    public NearCachePreloaderConfig getPreloaderConfig() {
        return preloaderConfig;
    }

    /**
     * Sets the {@link NearCachePreloaderConfig} of this Near Cache.
     *
     * @param preloaderConfig the {@link NearCachePreloaderConfig} of this Near Cache
     * @return this Near Cache config instance
     */
    public NearCacheConfig setPreloaderConfig(NearCachePreloaderConfig preloaderConfig) {
        this.preloaderConfig = checkNotNull(preloaderConfig, "NearCachePreloaderConfig cannot be null!");
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.NEAR_CACHE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(timeToLiveSeconds);
        out.writeInt(maxIdleSeconds);
        out.writeBoolean(invalidateOnChange);
        out.writeBoolean(cacheLocalEntries);
        out.writeInt(inMemoryFormat.ordinal());
        out.writeInt(localUpdatePolicy.ordinal());
        out.writeObject(evictionConfig);
        out.writeObject(preloaderConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        timeToLiveSeconds = in.readInt();
        maxIdleSeconds = in.readInt();
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
                + ", inMemoryFormat=" + inMemoryFormat
                + ", invalidateOnChange=" + invalidateOnChange
                + ", timeToLiveSeconds=" + timeToLiveSeconds
                + ", maxIdleSeconds=" + maxIdleSeconds
                + ", evictionConfig=" + evictionConfig
                + ", cacheLocalEntries=" + cacheLocalEntries
                + ", localUpdatePolicy=" + localUpdatePolicy
                + ", preloaderConfig=" + preloaderConfig
                + '}';
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NearCacheConfig that = (NearCacheConfig) o;

        if (serializeKeys != that.serializeKeys) {
            return false;
        }
        if (invalidateOnChange != that.invalidateOnChange) {
            return false;
        }
        if (timeToLiveSeconds != that.timeToLiveSeconds) {
            return false;
        }
        if (maxIdleSeconds != that.maxIdleSeconds) {
            return false;
        }
        if (cacheLocalEntries != that.cacheLocalEntries) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (inMemoryFormat != that.inMemoryFormat) {
            return false;
        }
        if (!Objects.equals(evictionConfig, that.evictionConfig)) {
            return false;
        }
        if (localUpdatePolicy != that.localUpdatePolicy) {
            return false;
        }
        return Objects.equals(preloaderConfig, that.preloaderConfig);
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + inMemoryFormat.hashCode();
        result = 31 * result + (serializeKeys ? 1 : 0);
        result = 31 * result + (invalidateOnChange ? 1 : 0);
        result = 31 * result + timeToLiveSeconds;
        result = 31 * result + maxIdleSeconds;
        result = 31 * result + (evictionConfig != null ? evictionConfig.hashCode() : 0);
        result = 31 * result + (cacheLocalEntries ? 1 : 0);
        result = 31 * result + (localUpdatePolicy != null ? localUpdatePolicy.hashCode() : 0);
        result = 31 * result + (preloaderConfig != null ? preloaderConfig.hashCode() : 0);
        return result;
    }
}
