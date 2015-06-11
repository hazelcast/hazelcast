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

import com.hazelcast.partition.InternalPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Simple configuration to hold parsed xml configuration.
 * CacheConfig depends on the JCache API. If the JCache API is not in the classpath,
 * you can use CacheSimpleConfig as a communicator between the code and CacheConfig.
 */
public class CacheSimpleConfig {

    /**
     * The minimum number of backups.
     */
    public static final int MIN_BACKUP_COUNT = 0;

    /**
     * The maximum number of backups.
     */
    public static final int MAX_BACKUP_COUNT = InternalPartition.MAX_BACKUP_COUNT;

    /**
     * The default number of backups.
     */
    public static final int DEFAULT_BACKUP_COUNT = 1;

    /**
     * Default InMemory Format.
     */
    public static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.BINARY;

    /**
     * Default Eviction Policy.
     */
    public static final EvictionPolicy DEFAULT_EVICTION_POLICY = EvictionConfig.DEFAULT_EVICTION_POLICY;

    private String name;

    private String keyType;
    private String valueType;

    private boolean statisticsEnabled;
    private boolean managementEnabled;

    private boolean readThrough;
    private boolean writeThrough;

    private String cacheLoaderFactory;
    private String cacheWriterFactory;

    private ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig;
    private List<CacheSimpleEntryListenerConfig> cacheEntryListeners;

    private int asyncBackupCount = MIN_BACKUP_COUNT;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    // Default value of eviction config is
    //      * ENTRY_COUNT with 10.000 max entry count
    //      * LRU as eviction policy
    private EvictionConfig evictionConfig = new EvictionConfig();
    private WanReplicationRef wanReplicationRef;

    private CacheSimpleConfig readOnly;

    public CacheSimpleConfig(CacheSimpleConfig cacheSimpleConfig) {
        this.name = cacheSimpleConfig.name;
        this.keyType = cacheSimpleConfig.keyType;
        this.valueType = cacheSimpleConfig.valueType;
        this.statisticsEnabled = cacheSimpleConfig.statisticsEnabled;
        this.managementEnabled = cacheSimpleConfig.managementEnabled;
        this.readThrough = cacheSimpleConfig.readThrough;
        this.writeThrough = cacheSimpleConfig.writeThrough;
        this.cacheLoaderFactory = cacheSimpleConfig.cacheLoaderFactory;
        this.cacheWriterFactory = cacheSimpleConfig.cacheWriterFactory;
        this.expiryPolicyFactoryConfig = cacheSimpleConfig.expiryPolicyFactoryConfig;
        this.cacheEntryListeners = cacheSimpleConfig.cacheEntryListeners;
        this.asyncBackupCount = cacheSimpleConfig.asyncBackupCount;
        this.backupCount = cacheSimpleConfig.backupCount;
        this.inMemoryFormat = cacheSimpleConfig.inMemoryFormat;
        // Eviction config cannot be null
        if (cacheSimpleConfig.evictionConfig != null) {
            this.evictionConfig = cacheSimpleConfig.evictionConfig;
        }
        this.wanReplicationRef = cacheSimpleConfig.wanReplicationRef;
    }

    public CacheSimpleConfig() {
    }

    /**
     * Gets the immutable version of this simple cache config.
     *
     * @return Immutable version of this simple cache config.
     */
    public CacheSimpleConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new CacheSimpleConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of this {@link com.hazelcast.cache.ICache}.
     *
     * @return the name of the {@link com.hazelcast.cache.ICache}.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this {@link com.hazelcast.cache.ICache}.
     *
     * @param name The name to set for this {@link com.hazelcast.cache.ICache}.
     * @return The current cache config instance.
     */
    public CacheSimpleConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Get the key type for this {@link com.hazelcast.cache.ICache}.
     *
     * @return The key type.
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * Sets the key type for this {@link com.hazelcast.cache.ICache}.
     *
     * @param keyType The key type to set for this {@link com.hazelcast.cache.ICache}.
     * @return The current cache config instance.
     */
    public CacheSimpleConfig setKeyType(String keyType) {
        this.keyType = keyType;
        return this;
    }

    /**
     * Get the value type for this {@link com.hazelcast.cache.ICache}.
     *
     * @return The value type for this {@link com.hazelcast.cache.ICache}.
     */
    public String getValueType() {
        return valueType;
    }

    /**
     * Sets the value type for this {@link com.hazelcast.cache.ICache}.
     *
     * @param valueType The value type to set for this {@link com.hazelcast.cache.ICache}.
     * @return The current cache config instance.
     */
    public CacheSimpleConfig setValueType(String valueType) {
        this.valueType = valueType;
        return this;
    }

    /**
     * Checks if statistics are enabled for this {@link com.hazelcast.cache.ICache}.
     *
     * @return True if statistics are enabled, false otherwise.
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Sets statistics to enabled or disabled for this {@link com.hazelcast.cache.ICache}.
     *
     * @param statisticsEnabled True to enable cache statistics, false to disable.
     * @return The current cache config instance.
     */
    public CacheSimpleConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * Checks if management is enabled for this {@link com.hazelcast.cache.ICache}.
     *
     * @return True if cache management is enabled, false otherwise.
     */
    public boolean isManagementEnabled() {
        return managementEnabled;
    }

    /**
     * Sets management to enabled or disabled for this {@link com.hazelcast.cache.ICache}.
     *
     * @param managementEnabled True to enable cache management, false to disable.
     * @return The current cache config instance.
     */
    public CacheSimpleConfig setManagementEnabled(boolean managementEnabled) {
        this.managementEnabled = managementEnabled;
        return this;
    }

    /**
     * Checks if this {@link com.hazelcast.cache.ICache} is read-through: a read loads the entry from the data store
     * if it is not already in the cache.
     *
     * @return True if the cache is read-through, false otherwise.
     */
    public boolean isReadThrough() {
        return readThrough;
    }

    /**
     * Enables or disables read-through: a read loads the entry from the data store if it is not already in the cache.
     *
     * @param readThrough True to enable read-through for this {@link com.hazelcast.cache.ICache}, false to disable.
     * @return The current cache config instance.
     */
    public CacheSimpleConfig setReadThrough(boolean readThrough) {
        this.readThrough = readThrough;
        return this;
    }

    /**
     * Checks if the {@link com.hazelcast.cache.ICache} is write-through: a write to the queue also loads the entry
     * into the data store.
     *
     * @return True if the cache is write-through, false otherwise.
     */
    public boolean isWriteThrough() {
        return writeThrough;
    }

    /**
     * Enables or disables write-through for this {@link com.hazelcast.cache.ICache}: a write to the queue also loads
     * the entry into the data store.
     *
     * @param writeThrough True to enable write-through, false to disable.
     * @return The current cache config instance.
     */
    public CacheSimpleConfig setWriteThrough(boolean writeThrough) {
        this.writeThrough = writeThrough;
        return this;
    }

    /**
     * Gets the factory for the {@link javax.cache.integration.CacheLoader}.
     *
     * @return The factory for the {@link javax.cache.integration.CacheLoader}.
     */
    public String getCacheLoaderFactory() {
        return cacheLoaderFactory;
    }

    /**
     * Sets the factory for this {@link javax.cache.integration.CacheLoader}.
     *
     * @param cacheLoaderFactory The factory to set for this {@link javax.cache.integration.CacheLoader}.
     * @return The current cache config instance.
     */
    public CacheSimpleConfig setCacheLoaderFactory(String cacheLoaderFactory) {
        this.cacheLoaderFactory = cacheLoaderFactory;
        return this;
    }

    /**
     * Gets the factory for the {@link javax.cache.integration.CacheWriter}.
     *
     * @return The factory for the {@link javax.cache.integration.CacheWriter}.
     */
    public String getCacheWriterFactory() {
        return cacheWriterFactory;
    }

    /**
     * Sets the factory for this {@link javax.cache.integration.CacheWriter}.
     *
     * @param cacheWriterFactory The factory to set for this {@link javax.cache.integration.CacheWriter}.
     * @return The current cache config instance.
     */
    public CacheSimpleConfig setCacheWriterFactory(String cacheWriterFactory) {
        this.cacheWriterFactory = cacheWriterFactory;
        return this;
    }

    /**
     * Gets the factory configuration for the {@link javax.cache.expiry.ExpiryPolicy}.
     *
     * @return The factory configuration for the {@link javax.cache.expiry.ExpiryPolicy}.
     */
    public ExpiryPolicyFactoryConfig getExpiryPolicyFactoryConfig() {
        return expiryPolicyFactoryConfig;
    }

    /**
     * Sets the factory configuration for this {@link javax.cache.expiry.ExpiryPolicy}.
     *
     * @param expiryPolicyFactoryConfig The factory configuration to set for this
     *                                  {@link javax.cache.expiry.ExpiryPolicy}.
     * @return The current cache config instance.
     */
    public CacheSimpleConfig setExpiryPolicyFactoryConfig(ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig) {
        this.expiryPolicyFactoryConfig = expiryPolicyFactoryConfig;
        return this;
    }

    /**
     * Sets the factory for this {@link javax.cache.expiry.ExpiryPolicy}.
     *
     * @param className The factory to set for this
     *                  {@link javax.cache.expiry.ExpiryPolicy}.
     * @return The current cache config instance.
     */
    public CacheSimpleConfig setExpiryPolicyFactory(String className) {
        this.expiryPolicyFactoryConfig = new ExpiryPolicyFactoryConfig(className);
        return this;
    }

    /**
     * Adds {@link CacheSimpleEntryListenerConfig} to this {@link com.hazelcast.cache.ICache}.
     *
     * @param listenerConfig
     * @return this {@code CacheSimpleConfig} instance.
     */
    public CacheSimpleConfig addEntryListenerConfig(CacheSimpleEntryListenerConfig listenerConfig) {
        getCacheEntryListeners().add(listenerConfig);
        return this;
    }

    /**
     * Gets a list of {@link CacheSimpleEntryListenerConfig} from this {@link com.hazelcast.cache.ICache}.
     *
     * @return list of {@link CacheSimpleEntryListenerConfig}.
     */
    public List<CacheSimpleEntryListenerConfig> getCacheEntryListeners() {
        if (cacheEntryListeners == null) {
            cacheEntryListeners = new ArrayList<CacheSimpleEntryListenerConfig>();
        }
        return cacheEntryListeners;
    }

    /**
     * Sets a list of {@link CacheSimpleEntryListenerConfig} for this {@link com.hazelcast.cache.ICache}.
     *
     * @param cacheEntryListeners list of {@link CacheSimpleEntryListenerConfig}.
     * @return this {@code CacheSimpleConfig} instance.
     */
    public CacheSimpleConfig setCacheEntryListeners(List<CacheSimpleEntryListenerConfig> cacheEntryListeners) {
        this.cacheEntryListeners = cacheEntryListeners;
        return this;
    }

    /**
     * Gets the number of asynchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @return The number of asynchronous backups for this {@link com.hazelcast.cache.ICache}.
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set.
     * @return the updated CacheSimpleConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than 0,
     *              or larger than the maximum number of backups,
     *              or the sum of the backups and async backups is larger than the maximum number of backups.
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public CacheSimpleConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Gets the number of synchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @return The number of synchronous backups.
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of synchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @param backupCount the new backupCount
     * @return the updated CacheSimpleConfig
     * @throws new IllegalArgumentException if backupCount smaller than 0,
     *             or larger than the maximum number of backup
     *             or the sum of the backups and async backups is larger than the maximum number of backups
     */
    public CacheSimpleConfig setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Gets the InMemory Format for this {@link com.hazelcast.cache.ICache}.
     *
     * @return The InMemory Format.
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Sets the InMemory Format for this {@link com.hazelcast.cache.ICache}.
     *
     * @param inMemoryFormat The InMemory Format.
     * @return the updated CacheSimpleConfig.
     */
    public CacheSimpleConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat, "In-Memory format cannot be null !");
        return this;
    }

    /**
     * Gets the eviction configuration for this {@link com.hazelcast.cache.ICache}.
     *
     * @return The eviction configuration.
     */
    public EvictionConfig getEvictionConfig() {
        return evictionConfig;
    }

    /**
     * Sets the eviction configuration for this {@link com.hazelcast.cache.ICache}.
     *
     * @param evictionConfig The eviction configuration to set.
     * @return the updated CacheSimpleConfig.
     */
    public CacheSimpleConfig setEvictionConfig(EvictionConfig evictionConfig) {
        this.evictionConfig = isNotNull(evictionConfig, "Eviction config cannot be null !");
        return this;
    }

    /**
     * Gets the Wan target replication reference.
     *
     * @return The Wan target replication reference.
     */
    public WanReplicationRef getWanReplicationRef() {
        return wanReplicationRef;
    }

    /**
     * Sets the Wan target replication reference.
     *
     * @param wanReplicationRef the Wan target replication reference.
     */
    public void setWanReplicationRef(WanReplicationRef wanReplicationRef) {
        this.wanReplicationRef = wanReplicationRef;
    }

    /**
     * Represents configuration for "ExpiryPolicyFactory".
     */
    public static class ExpiryPolicyFactoryConfig {

        private final String className;
        private final TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig;

        public ExpiryPolicyFactoryConfig(String className) {
            this.className = className;
            this.timedExpiryPolicyFactoryConfig = null;
        }

        public ExpiryPolicyFactoryConfig(TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig) {
            this.className = null;
            this.timedExpiryPolicyFactoryConfig = timedExpiryPolicyFactoryConfig;
        }

        public String getClassName() {
            return className;
        }

        public TimedExpiryPolicyFactoryConfig getTimedExpiryPolicyFactoryConfig() {
            return timedExpiryPolicyFactoryConfig;
        }

        /**
         * Represents configuration for time based "ExpiryPolicyFactory" with duration and time unit.
         */
        public static class TimedExpiryPolicyFactoryConfig {

            private final ExpiryPolicyType expiryPolicyType;
            private final DurationConfig durationConfig;

            public TimedExpiryPolicyFactoryConfig(ExpiryPolicyType expiryPolicyType,
                                                  DurationConfig durationConfig) {
                this.expiryPolicyType = expiryPolicyType;
                this.durationConfig = durationConfig;
            }

            public ExpiryPolicyType getExpiryPolicyType() {
                return expiryPolicyType;
            }

            public DurationConfig getDurationConfig() {
                return durationConfig;
            }

            /**
             * Represents type of the "TimedExpiryPolicyFactoryConfig".
             */
            public enum ExpiryPolicyType {

                /**
                 * Expiry policy type for the "javax.cache.expiry.AccessedExpiryPolicy"
                 */
                CREATED,
                /**
                 * Expiry policy type for the "javax.cache.expiry.ModifiedExpiryPolicy"
                 */
                MODIFIED,
                /**
                 * Expiry policy type for the "javax.cache.expiry.AccessedExpiryPolicy"
                 */
                ACCESSED,
                /**
                 * Expiry policy type for the "javax.cache.expiry.TouchedExpiryPolicy"
                 */
                TOUCHED,
                /**
                 * Expiry policy type for the "javax.cache.expiry.EternalExpiryPolicy"
                 */
                ETERNAL

            }

        }

        /**
         * Represents duration configuration with duration amount and time unit
         * for the "TimedExpiryPolicyFactoryConfig".
         */
        public static class DurationConfig {

            private final long durationAmount;
            private final TimeUnit timeUnit;

            public DurationConfig(long durationAmount, TimeUnit timeUnit) {
                this.durationAmount = durationAmount;
                this.timeUnit = timeUnit;
            }

            public long getDurationAmount() {
                return durationAmount;
            }

            public TimeUnit getTimeUnit() {
                return timeUnit;
            }

        }

    }

}
