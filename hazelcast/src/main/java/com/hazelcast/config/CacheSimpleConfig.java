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

import com.hazelcast.cache.BuiltInCacheMergePolicies;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.partition.IPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Simple configuration to hold parsed XML configuration.
 * CacheConfig depends on the JCache API. If the JCache API is not in the classpath,
 * you can use CacheSimpleConfig as a communicator between the code and CacheConfig.
 */
public class CacheSimpleConfig implements IdentifiedDataSerializable {

    /**
     * The minimum number of backups.
     */
    public static final int MIN_BACKUP_COUNT = 0;

    /**
     * The maximum number of backups.
     */
    public static final int MAX_BACKUP_COUNT = IPartition.MAX_BACKUP_COUNT;

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

    /**
     * Default policy for merging
     */
    public static final String DEFAULT_CACHE_MERGE_POLICY = PassThroughCacheMergePolicy.class.getName();

    private String name;

    private String keyType;
    private String valueType;

    private boolean statisticsEnabled;
    private boolean managementEnabled;

    private boolean readThrough;
    private boolean writeThrough;

    private String cacheLoaderFactory;
    private String cacheWriterFactory;

    private String cacheLoader;
    private String cacheWriter;

    private ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig;
    private List<CacheSimpleEntryListenerConfig> cacheEntryListeners;

    private int asyncBackupCount = MIN_BACKUP_COUNT;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    // Default values of the eviction configuration are:
    //      * ENTRY_COUNT with 10.000 as the max-size policy.
    //      * LRU as the eviction-policy.
    private EvictionConfig evictionConfig = new EvictionConfig();
    private WanReplicationRef wanReplicationRef;

    private transient CacheSimpleConfig readOnly;

    private String quorumName;

    private List<CachePartitionLostListenerConfig> partitionLostListenerConfigs;

    private String mergePolicy = BuiltInCacheMergePolicies.getDefault().getImplementationClassName();

    private HotRestartConfig hotRestartConfig = new HotRestartConfig();

    /**
     * Disables invalidation events for per entry but full-flush invalidation events are still enabled.
     * Full-flush invalidation means the invalidation of events for all entries when clear is called.
     */
    private boolean disablePerEntryInvalidationEvents;

    @SuppressWarnings("checkstyle:executablestatementcount")
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
        // eviction config cannot be null
        if (cacheSimpleConfig.evictionConfig != null) {
            this.evictionConfig = cacheSimpleConfig.evictionConfig;
        }
        this.wanReplicationRef = cacheSimpleConfig.wanReplicationRef;
        this.partitionLostListenerConfigs =
                new ArrayList<CachePartitionLostListenerConfig>(cacheSimpleConfig.getPartitionLostListenerConfigs());
        this.quorumName = cacheSimpleConfig.quorumName;
        this.mergePolicy = cacheSimpleConfig.mergePolicy;
        this.hotRestartConfig = new HotRestartConfig(cacheSimpleConfig.hotRestartConfig);
        this.disablePerEntryInvalidationEvents = cacheSimpleConfig.disablePerEntryInvalidationEvents;
    }

    public CacheSimpleConfig() {
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
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
     * @return the name of the {@link com.hazelcast.cache.ICache}
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this {@link com.hazelcast.cache.ICache}.
     *
     * @param name the name to set for this {@link com.hazelcast.cache.ICache}
     * @return the current cache config instance
     */
    public CacheSimpleConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the key type for this {@link com.hazelcast.cache.ICache}.
     *
     * @return the key type
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * Sets the key type for this {@link com.hazelcast.cache.ICache}.
     *
     * @param keyType the key type to set for this {@link com.hazelcast.cache.ICache}
     * @return the current cache config instance
     */
    public CacheSimpleConfig setKeyType(String keyType) {
        this.keyType = keyType;
        return this;
    }

    /**
     * Gets the value type for this {@link com.hazelcast.cache.ICache}.
     *
     * @return the value type for this {@link com.hazelcast.cache.ICache}
     */
    public String getValueType() {
        return valueType;
    }

    /**
     * Sets the value type for this {@link com.hazelcast.cache.ICache}.
     *
     * @param valueType the value type to set for this {@link com.hazelcast.cache.ICache}
     * @return the current cache config instance
     */
    public CacheSimpleConfig setValueType(String valueType) {
        this.valueType = valueType;
        return this;
    }

    /**
     * Checks if statistics are enabled for this {@link com.hazelcast.cache.ICache}.
     *
     * @return {@code true} if statistics are enabled, {@code false} otherwise
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Sets statistics to enabled or disabled for this {@link com.hazelcast.cache.ICache}.
     *
     * @param statisticsEnabled {@code true} to enable cache statistics, {@code false} to disable
     * @return the current cache config instance
     */
    public CacheSimpleConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * Checks if management is enabled for this {@link com.hazelcast.cache.ICache}.
     *
     * @return {@code true} if cache management is enabled, {@code false} otherwise
     */
    public boolean isManagementEnabled() {
        return managementEnabled;
    }

    /**
     * Sets management to enabled or disabled for this {@link com.hazelcast.cache.ICache}.
     *
     * @param managementEnabled {@code true} to enable cache management, {@code false} to disable
     * @return the current cache config instance
     */
    public CacheSimpleConfig setManagementEnabled(boolean managementEnabled) {
        this.managementEnabled = managementEnabled;
        return this;
    }

    /**
     * Checks if this {@link com.hazelcast.cache.ICache} is read-through: a read loads the entry from the data store
     * if it is not already in the cache.
     *
     * @return {@code true} if the cache is read-through, {@code false} otherwise
     */
    public boolean isReadThrough() {
        return readThrough;
    }

    /**
     * Enables or disables read-through: a read loads the entry from the data store if it is not already in the cache.
     *
     * @param readThrough {@code true} to enable read-through for this {@link com.hazelcast.cache.ICache},
     *                    {@code false} to disable
     * @return the current cache config instance
     */
    public CacheSimpleConfig setReadThrough(boolean readThrough) {
        this.readThrough = readThrough;
        return this;
    }

    /**
     * Checks if the {@link com.hazelcast.cache.ICache} is write-through: a write to the queue also loads the entry
     * into the data store.
     *
     * @return {@code true} if the cache is write-through, {@code false} otherwise
     */
    public boolean isWriteThrough() {
        return writeThrough;
    }

    /**
     * Enables or disables write-through for this {@link com.hazelcast.cache.ICache}: a write to the queue also loads
     * the entry into the data store.
     *
     * @param writeThrough {@code true} to enable write-through, {@code false} to disable
     * @return the current cache config instance
     */
    public CacheSimpleConfig setWriteThrough(boolean writeThrough) {
        this.writeThrough = writeThrough;
        return this;
    }

    /**
     * Gets the factory for the {@link javax.cache.integration.CacheLoader}.
     *
     * @return the factory for the {@link javax.cache.integration.CacheLoader}
     */
    public String getCacheLoaderFactory() {
        return cacheLoaderFactory;
    }

    /**
     * Sets the factory for this {@link javax.cache.integration.CacheLoader}.
     *
     * @param cacheLoaderFactory the factory to set for this {@link javax.cache.integration.CacheLoader}
     * @return the current cache config instance
     */
    public CacheSimpleConfig setCacheLoaderFactory(String cacheLoaderFactory) {
        if (cacheLoader != null && cacheLoaderFactory != null) {
            throw new IllegalStateException("Cannot set cacheLoaderFactory to '" + cacheLoaderFactory
                    + "', because cacheLoader is already set to '" + cacheLoader + "'.");
        }
        this.cacheLoaderFactory = cacheLoaderFactory;
        return this;
    }

    /**
     * Get classname of a class to be used as {@link javax.cache.integration.CacheLoader}.
     *
     * @return classname to be used as {@link javax.cache.integration.CacheLoader}
     */
    public String getCacheLoader() {
        return cacheLoader;
    }

    /**
     * Set classname of a class to be used as {@link javax.cache.integration.CacheLoader}.
     *
     * @param cacheLoader classname to be used as {@link javax.cache.integration.CacheLoader}
     * @return the current cache config instance
     */
    public CacheSimpleConfig setCacheLoader(String cacheLoader) {
        if (cacheLoader != null && cacheLoaderFactory != null) {
            throw new IllegalStateException("Cannot set cacheLoader to '" + cacheLoader
                    + "', because cacheLoaderFactory is already set to '" + cacheLoaderFactory + "'.");
        }
        this.cacheLoader = cacheLoader;
        return this;
    }

    /**
     * Gets the factory for the {@link javax.cache.integration.CacheWriter}.
     *
     * @return the factory for the {@link javax.cache.integration.CacheWriter}
     */
    public String getCacheWriterFactory() {
        return cacheWriterFactory;
    }

    /**
     * Sets the factory for this {@link javax.cache.integration.CacheWriter}.
     *
     * @param cacheWriterFactory the factory to set for this {@link javax.cache.integration.CacheWriter}
     * @return the current cache config instance
     */
    public CacheSimpleConfig setCacheWriterFactory(String cacheWriterFactory) {
        if (cacheWriter != null && cacheWriterFactory != null) {
            throw new IllegalStateException("Cannot set cacheWriterFactory to '" + cacheWriterFactory
                    + "', because cacheWriter is already set to '" + cacheWriter + "'.");
        }
        this.cacheWriterFactory = cacheWriterFactory;
        return this;
    }

    /**
     * Get classname of a class to be used as {@link javax.cache.integration.CacheWriter}.
     *
     * @return classname to be used as {@link javax.cache.integration.CacheWriter}
     */
    public String getCacheWriter() {
        return cacheWriter;
    }

    /**
     * Set classname of a class to be used as {@link javax.cache.integration.CacheWriter}.
     *
     * @param cacheWriter classname to be used as {@link javax.cache.integration.CacheWriter}
     * @return the current cache config instance
     */
    public CacheSimpleConfig setCacheWriter(String cacheWriter) {
        if (cacheWriter != null && cacheWriterFactory != null) {
            throw new IllegalStateException("Cannot set cacheWriter to '" + cacheWriter
                    + "', because cacheWriterFactory is already set to '" + cacheWriterFactory + "'.");
        }
        this.cacheWriter = cacheWriter;
        return this;
    }

    /**
     * Gets the factory configuration for the {@link javax.cache.expiry.ExpiryPolicy}.
     *
     * @return the factory configuration for the {@link javax.cache.expiry.ExpiryPolicy}
     */
    public ExpiryPolicyFactoryConfig getExpiryPolicyFactoryConfig() {
        return expiryPolicyFactoryConfig;
    }

    /**
     * Sets the factory configuration for this {@link javax.cache.expiry.ExpiryPolicy}.
     *
     * @param expiryPolicyFactoryConfig the factory configuration to set for this {@link javax.cache.expiry.ExpiryPolicy}
     * @return the current cache config instance
     */
    public CacheSimpleConfig setExpiryPolicyFactoryConfig(ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig) {
        this.expiryPolicyFactoryConfig = expiryPolicyFactoryConfig;
        return this;
    }

    /**
     * Sets the factory for this {@link javax.cache.expiry.ExpiryPolicy}.
     *
     * @param className the factory to set for this {@link javax.cache.expiry.ExpiryPolicy}
     * @return the current cache config instance
     */
    public CacheSimpleConfig setExpiryPolicyFactory(String className) {
        this.expiryPolicyFactoryConfig = new ExpiryPolicyFactoryConfig(className);
        return this;
    }

    /**
     * Adds {@link CacheSimpleEntryListenerConfig} to this {@link com.hazelcast.cache.ICache}.
     *
     * @return this {@code CacheSimpleConfig} instance
     */
    public CacheSimpleConfig addEntryListenerConfig(CacheSimpleEntryListenerConfig listenerConfig) {
        getCacheEntryListeners().add(listenerConfig);
        return this;
    }

    /**
     * Gets a list of {@link CacheSimpleEntryListenerConfig} from this {@link com.hazelcast.cache.ICache}.
     *
     * @return list of {@link CacheSimpleEntryListenerConfig}
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
     * @param cacheEntryListeners list of {@link CacheSimpleEntryListenerConfig}
     * @return this {@code CacheSimpleConfig} instance
     */
    public CacheSimpleConfig setCacheEntryListeners(List<CacheSimpleEntryListenerConfig> cacheEntryListeners) {
        this.cacheEntryListeners = cacheEntryListeners;
        return this;
    }

    /**
     * Gets the number of asynchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @return the number of asynchronous backups for this {@link com.hazelcast.cache.ICache}
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set
     * @return the updated CacheSimpleConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than 0,
     *                                  or larger than the maximum number of backups,
     *                                  or the sum of the backups and async backups is larger than the maximum number of backups
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
     * @return the number of synchronous backups
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of synchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @param backupCount the new backupCount
     * @return the updated CacheSimpleConfig
     * @throws IllegalArgumentException if backupCount smaller than 0,
     *                                  or larger than the maximum number of backup
     *                                  or the sum of the backups and async backups is larger
     *                                  than the maximum number of backups
     */
    public CacheSimpleConfig setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Gets the InMemory Format for this {@link com.hazelcast.cache.ICache}.
     *
     * @return the InMemory Format
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Sets the InMemory Format for this {@link com.hazelcast.cache.ICache}.
     *
     * @param inMemoryFormat the InMemory Format
     * @return the updated CacheSimpleConfig
     */
    public CacheSimpleConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat, "In-Memory format cannot be null!");
        return this;
    }

    /**
     * Gets the eviction configuration for this {@link com.hazelcast.cache.ICache}.
     *
     * @return the eviction configuration
     */
    public EvictionConfig getEvictionConfig() {
        return evictionConfig;
    }

    /**
     * Sets the eviction configuration for this {@link com.hazelcast.cache.ICache}.
     *
     * @param evictionConfig the eviction configuration to set
     * @return the updated CacheSimpleConfig
     */
    public CacheSimpleConfig setEvictionConfig(EvictionConfig evictionConfig) {
        this.evictionConfig = isNotNull(evictionConfig, "evictionConfig");
        return this;
    }

    /**
     * Gets the WAN target replication reference.
     *
     * @return the WAN target replication reference
     */
    public WanReplicationRef getWanReplicationRef() {
        return wanReplicationRef;
    }

    /**
     * Sets the WAN target replication reference.
     *
     * @param wanReplicationRef the WAN target replication reference
     */
    public void setWanReplicationRef(WanReplicationRef wanReplicationRef) {
        this.wanReplicationRef = wanReplicationRef;
    }

    /**
     * Gets the partition lost listener references added to cache configuration.
     *
     * @return List of CachePartitionLostListenerConfig
     */
    public List<CachePartitionLostListenerConfig> getPartitionLostListenerConfigs() {
        if (partitionLostListenerConfigs == null) {
            partitionLostListenerConfigs = new ArrayList<CachePartitionLostListenerConfig>();
        }
        return partitionLostListenerConfigs;
    }

    /**
     * Sets the PartitionLostListenerConfigs.
     *
     * @param partitionLostListenerConfigs CachePartitionLostListenerConfig list
     */
    public CacheSimpleConfig setPartitionLostListenerConfigs(
            List<CachePartitionLostListenerConfig> partitionLostListenerConfigs) {
        this.partitionLostListenerConfigs = partitionLostListenerConfigs;
        return this;
    }

    /**
     * Adds the CachePartitionLostListenerConfig to partitionLostListenerConfigs.
     *
     * @param listenerConfig CachePartitionLostListenerConfig to be added
     */
    public CacheSimpleConfig addCachePartitionLostListenerConfig(CachePartitionLostListenerConfig listenerConfig) {
        getPartitionLostListenerConfigs().add(listenerConfig);
        return this;
    }

    /**
     * Gets the name of the associated quorum if any.
     *
     * @return the name of the associated quorum if any
     */
    public String getQuorumName() {
        return quorumName;
    }

    /**
     * Associates this cache configuration to a quorum.
     *
     * @param quorumName name of the desired quorum
     * @return the updated CacheSimpleConfig
     */
    public CacheSimpleConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }

    /**
     * Gets the class name of {@link com.hazelcast.cache.CacheMergePolicy} implementation of this cache config.
     *
     * @return the class name of {@link com.hazelcast.cache.CacheMergePolicy} implementation of this cache config
     */
    public String getMergePolicy() {
        return mergePolicy;
    }

    /**
     * Sets the class name of {@link com.hazelcast.cache.CacheMergePolicy} implementation to this cache config.
     *
     * @param mergePolicy the class name of {@link com.hazelcast.cache.CacheMergePolicy} implementation
     *                    to be set to this cache config
     */
    public void setMergePolicy(String mergePolicy) {
        this.mergePolicy = mergePolicy;
    }

    /**
     * Gets the {@code HotRestartConfig} for this {@code CacheSimpleConfig}
     *
     * @return hot restart config
     */
    public HotRestartConfig getHotRestartConfig() {
        return hotRestartConfig;
    }

    /**
     * Sets the {@code HotRestartConfig} for this {@code CacheSimpleConfig}
     *
     * @param hotRestartConfig hot restart config
     * @return this {@code CacheSimpleConfig} instance
     */
    public CacheSimpleConfig setHotRestartConfig(HotRestartConfig hotRestartConfig) {
        this.hotRestartConfig = hotRestartConfig;
        return this;
    }

    /**
     * Returns invalidation events disabled status for per entry.
     *
     * @return {@code true} if invalidation events are disabled for per entry, {@code false} otherwise
     */
    public boolean isDisablePerEntryInvalidationEvents() {
        return disablePerEntryInvalidationEvents;
    }

    /**
     * Sets invalidation events disabled status for per entry.
     *
     * @param disablePerEntryInvalidationEvents Disables invalidation event sending behaviour if it is {@code true},
     *                                          otherwise enables it
     */
    public void setDisablePerEntryInvalidationEvents(boolean disablePerEntryInvalidationEvents) {
        this.disablePerEntryInvalidationEvents = disablePerEntryInvalidationEvents;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.SIMPLE_CACHE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(keyType);
        out.writeUTF(valueType);
        out.writeBoolean(statisticsEnabled);
        out.writeBoolean(managementEnabled);
        out.writeBoolean(readThrough);
        out.writeBoolean(writeThrough);
        out.writeBoolean(disablePerEntryInvalidationEvents);
        out.writeUTF(cacheLoaderFactory);
        out.writeUTF(cacheWriterFactory);
        out.writeUTF(cacheLoader);
        out.writeUTF(cacheWriter);
        out.writeObject(expiryPolicyFactoryConfig);
        writeNullableList(cacheEntryListeners, out);
        out.writeInt(asyncBackupCount);
        out.writeInt(backupCount);
        out.writeUTF(inMemoryFormat.name());
        out.writeObject(evictionConfig);
        out.writeObject(wanReplicationRef);
        out.writeUTF(quorumName);
        writeNullableList(partitionLostListenerConfigs, out);
        out.writeUTF(mergePolicy);
        out.writeObject(hotRestartConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        keyType = in.readUTF();
        valueType = in.readUTF();
        statisticsEnabled = in.readBoolean();
        managementEnabled = in.readBoolean();
        readThrough = in.readBoolean();
        writeThrough = in.readBoolean();
        disablePerEntryInvalidationEvents = in.readBoolean();
        cacheLoaderFactory = in.readUTF();
        cacheWriterFactory = in.readUTF();
        cacheLoader = in.readUTF();
        cacheWriter = in.readUTF();
        expiryPolicyFactoryConfig = in.readObject();
        cacheEntryListeners = readNullableList(in);
        asyncBackupCount = in.readInt();
        backupCount = in.readInt();
        inMemoryFormat = InMemoryFormat.valueOf(in.readUTF());
        evictionConfig = in.readObject();
        wanReplicationRef = in.readObject();
        quorumName = in.readUTF();
        partitionLostListenerConfigs = readNullableList(in);
        mergePolicy = in.readUTF();
        hotRestartConfig = in.readObject();
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:methodlength"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CacheSimpleConfig that = (CacheSimpleConfig) o;

        if (statisticsEnabled != that.statisticsEnabled) {
            return false;
        }
        if (managementEnabled != that.managementEnabled) {
            return false;
        }
        if (readThrough != that.readThrough) {
            return false;
        }
        if (writeThrough != that.writeThrough) {
            return false;
        }
        if (asyncBackupCount != that.asyncBackupCount) {
            return false;
        }
        if (backupCount != that.backupCount) {
            return false;
        }
        if (disablePerEntryInvalidationEvents != that.disablePerEntryInvalidationEvents) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (keyType != null ? !keyType.equals(that.keyType) : that.keyType != null) {
            return false;
        }
        if (valueType != null ? !valueType.equals(that.valueType) : that.valueType != null) {
            return false;
        }
        if (cacheLoaderFactory != null
                ? !cacheLoaderFactory.equals(that.cacheLoaderFactory) : that.cacheLoaderFactory != null) {
            return false;
        }
        if (cacheWriterFactory != null
                ? !cacheWriterFactory.equals(that.cacheWriterFactory) : that.cacheWriterFactory != null) {
            return false;
        }
        if (cacheLoader != null ? !cacheLoader.equals(that.cacheLoader) : that.cacheLoader != null) {
            return false;
        }
        if (cacheWriter != null ? !cacheWriter.equals(that.cacheWriter) : that.cacheWriter != null) {
            return false;
        }
        if (expiryPolicyFactoryConfig != null
                ? !expiryPolicyFactoryConfig.equals(that.expiryPolicyFactoryConfig)
                : that.expiryPolicyFactoryConfig != null) {
            return false;
        }
        if (cacheEntryListeners != null ? !cacheEntryListeners.equals(that.cacheEntryListeners)
                : that.cacheEntryListeners != null) {
            return false;
        }
        if (inMemoryFormat != that.inMemoryFormat) {
            return false;
        }
        if (evictionConfig != null ? !evictionConfig.equals(that.evictionConfig) : that.evictionConfig != null) {
            return false;
        }
        if (wanReplicationRef != null ? !wanReplicationRef.equals(that.wanReplicationRef)
                : that.wanReplicationRef != null) {
            return false;
        }
        if (quorumName != null ? !quorumName.equals(that.quorumName) : that.quorumName != null) {
            return false;
        }
        if (partitionLostListenerConfigs != null
                ? !partitionLostListenerConfigs.equals(that.partitionLostListenerConfigs)
                : that.partitionLostListenerConfigs != null) {
            return false;
        }
        if (mergePolicy != null ? !mergePolicy.equals(that.mergePolicy) : that.mergePolicy != null) {
            return false;
        }
        return hotRestartConfig != null ? hotRestartConfig.equals(that.hotRestartConfig) : that.hotRestartConfig == null;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (keyType != null ? keyType.hashCode() : 0);
        result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        result = 31 * result + (managementEnabled ? 1 : 0);
        result = 31 * result + (readThrough ? 1 : 0);
        result = 31 * result + (writeThrough ? 1 : 0);
        result = 31 * result + (cacheLoaderFactory != null ? cacheLoaderFactory.hashCode() : 0);
        result = 31 * result + (cacheWriterFactory != null ? cacheWriterFactory.hashCode() : 0);
        result = 31 * result + (cacheLoader != null ? cacheLoader.hashCode() : 0);
        result = 31 * result + (cacheWriter != null ? cacheWriter.hashCode() : 0);
        result = 31 * result + (expiryPolicyFactoryConfig != null ? expiryPolicyFactoryConfig.hashCode() : 0);
        result = 31 * result + (cacheEntryListeners != null ? cacheEntryListeners.hashCode() : 0);
        result = 31 * result + asyncBackupCount;
        result = 31 * result + backupCount;
        result = 31 * result + (inMemoryFormat != null ? inMemoryFormat.hashCode() : 0);
        result = 31 * result + (evictionConfig != null ? evictionConfig.hashCode() : 0);
        result = 31 * result + (wanReplicationRef != null ? wanReplicationRef.hashCode() : 0);
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        result = 31 * result + (partitionLostListenerConfigs != null ? partitionLostListenerConfigs.hashCode() : 0);
        result = 31 * result + (mergePolicy != null ? mergePolicy.hashCode() : 0);
        result = 31 * result + (hotRestartConfig != null ? hotRestartConfig.hashCode() : 0);
        result = 31 * result + (disablePerEntryInvalidationEvents ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CacheSimpleConfig{"
                + "name='" + name + '\''
                + ", asyncBackupCount=" + asyncBackupCount
                + ", backupCount=" + backupCount
                + ", inMemoryFormat=" + inMemoryFormat
                + ", keyType=" + keyType
                + ", valueType=" + valueType
                + ", statisticsEnabled=" + statisticsEnabled
                + ", managementEnabled=" + managementEnabled
                + ", readThrough=" + readThrough
                + ", writeThrough=" + writeThrough
                + ", cacheLoaderFactory='" + cacheLoaderFactory + '\''
                + ", cacheWriterFactory='" + cacheWriterFactory + '\''
                + ", cacheLoader='" + cacheLoader + '\''
                + ", cacheWriter='" + cacheWriter + '\''
                + ", expiryPolicyFactoryConfig=" + expiryPolicyFactoryConfig
                + ", cacheEntryListeners=" + cacheEntryListeners
                + ", evictionConfig=" + evictionConfig
                + ", wanReplicationRef=" + wanReplicationRef
                + ", quorumName=" + quorumName
                + ", partitionLostListenerConfigs=" + partitionLostListenerConfigs
                + ", mergePolicy=" + mergePolicy
                + ", hotRestartConfig=" + hotRestartConfig
                + '}';
    }

    /**
     * Represents configuration for "ExpiryPolicyFactory".
     */
    public static class ExpiryPolicyFactoryConfig implements IdentifiedDataSerializable {

        private String className;
        private TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig;

        public ExpiryPolicyFactoryConfig() {
        }

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

        @Override
        public int getFactoryId() {
            return ConfigDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return ConfigDataSerializerHook.SIMPLE_CACHE_CONFIG_EXPIRY_POLICY_FACTORY_CONFIG;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(className);
            out.writeObject(timedExpiryPolicyFactoryConfig);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            className = in.readUTF();
            timedExpiryPolicyFactoryConfig = in.readObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ExpiryPolicyFactoryConfig that = (ExpiryPolicyFactoryConfig) o;

            if (className != null ? !className.equals(that.className) : that.className != null) {
                return false;
            }
            return timedExpiryPolicyFactoryConfig != null
                    ? timedExpiryPolicyFactoryConfig.equals(that.timedExpiryPolicyFactoryConfig)
                    : that.timedExpiryPolicyFactoryConfig == null;
        }

        @Override
        public int hashCode() {
            int result = className != null ? className.hashCode() : 0;
            result = 31 * result + (timedExpiryPolicyFactoryConfig != null ? timedExpiryPolicyFactoryConfig.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ExpiryPolicyFactoryConfig{"
                    + "className='" + className + '\''
                    + ", timedExpiryPolicyFactoryConfig=" + timedExpiryPolicyFactoryConfig
                    + '}';
        }

        /**
         * Represents configuration for time based "ExpiryPolicyFactory" with duration and time unit.
         */
        public static class TimedExpiryPolicyFactoryConfig implements IdentifiedDataSerializable {

            private ExpiryPolicyType expiryPolicyType;
            private DurationConfig durationConfig;

            public TimedExpiryPolicyFactoryConfig() {
            }

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

            @Override
            public int getFactoryId() {
                return ConfigDataSerializerHook.F_ID;
            }

            @Override
            public int getId() {
                return ConfigDataSerializerHook.SIMPLE_CACHE_CONFIG_TIMED_EXPIRY_POLICY_FACTORY_CONFIG;
            }

            @Override
            public void writeData(ObjectDataOutput out) throws IOException {
                out.writeUTF(expiryPolicyType.name());
                out.writeObject(durationConfig);
            }

            @Override
            public void readData(ObjectDataInput in) throws IOException {
                expiryPolicyType = ExpiryPolicyType.valueOf(in.readUTF());
                durationConfig = in.readObject();
            }

            /**
             * Represents type of the "TimedExpiryPolicyFactoryConfig".
             */
            public enum ExpiryPolicyType {

                /**
                 * Expiry policy type for the {@link javax.cache.expiry.CreatedExpiryPolicy}.
                 */
                CREATED,
                /**
                 * Expiry policy type for the {@link javax.cache.expiry.ModifiedExpiryPolicy}.
                 */
                MODIFIED,
                /**
                 * Expiry policy type for the {@link javax.cache.expiry.AccessedExpiryPolicy}.
                 */
                ACCESSED,
                /**
                 * Expiry policy type for the {@link javax.cache.expiry.TouchedExpiryPolicy}.
                 */
                TOUCHED,
                /**
                 * Expiry policy type for the {@link javax.cache.expiry.EternalExpiryPolicy}.
                 */
                ETERNAL
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                TimedExpiryPolicyFactoryConfig that = (TimedExpiryPolicyFactoryConfig) o;
                if (expiryPolicyType != that.expiryPolicyType) {
                    return false;
                }
                return durationConfig != null ? durationConfig.equals(that.durationConfig) : that.durationConfig == null;
            }

            @Override
            public int hashCode() {
                int result = expiryPolicyType != null ? expiryPolicyType.hashCode() : 0;
                result = 31 * result + (durationConfig != null ? durationConfig.hashCode() : 0);
                return result;
            }

            @Override
            public String toString() {
                return "TimedExpiryPolicyFactoryConfig{"
                        + "expiryPolicyType=" + expiryPolicyType
                        + ", durationConfig=" + durationConfig
                        + '}';
            }
        }

        /**
         * Represents duration configuration with duration amount and time unit
         * for the "TimedExpiryPolicyFactoryConfig".
         */
        public static class DurationConfig implements IdentifiedDataSerializable {

            private long durationAmount;
            private TimeUnit timeUnit;

            public DurationConfig() {

            }

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

            @Override
            public int getFactoryId() {
                return ConfigDataSerializerHook.F_ID;
            }

            @Override
            public int getId() {
                return ConfigDataSerializerHook.SIMPLE_CACHE_CONFIG_DURATION_CONFIG;
            }

            @Override
            public void writeData(ObjectDataOutput out) throws IOException {
                out.writeLong(durationAmount);
                out.writeUTF(timeUnit.name());
            }

            @Override
            public void readData(ObjectDataInput in) throws IOException {
                durationAmount = in.readLong();
                timeUnit = TimeUnit.valueOf(in.readUTF());
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                DurationConfig that = (DurationConfig) o;

                if (durationAmount != that.durationAmount) {
                    return false;
                }
                return timeUnit == that.timeUnit;
            }

            @Override
            public int hashCode() {
                int result = (int) (durationAmount ^ (durationAmount >>> 32));
                result = 31 * result + (timeUnit != null ? timeUnit.hashCode() : 0);
                return result;
            }

            @Override
            public String toString() {
                return "DurationConfig{"
                        + "durationAmount=" + durationAmount
                        + ", timeUnit" + timeUnit
                        + '}';
            }
        }
    }
}
