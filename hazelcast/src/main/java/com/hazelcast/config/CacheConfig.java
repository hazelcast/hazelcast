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

import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.config.CacheSimpleConfig.DEFAULT_BACKUP_COUNT;
import static com.hazelcast.config.CacheSimpleConfig.DEFAULT_IN_MEMORY_FORMAT;
import static com.hazelcast.config.CacheSimpleConfig.MIN_BACKUP_COUNT;
import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains all the configuration for the {@link com.hazelcast.cache.ICache}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
public class CacheConfig<K, V>
        extends AbstractCacheConfig<K, V> {

    private String name;
    private String managerPrefix;
    private String uriString;
    private int asyncBackupCount = MIN_BACKUP_COUNT;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    // Default value of eviction config is
    //      * ENTRY_COUNT with 10.000 max entry count
    //      * LRU as eviction policy
    // TODO Change to "EvictionConfig" instead of "CacheEvictionConfig" in the future
    // since "CacheEvictionConfig" is deprecated
    private CacheEvictionConfig evictionConfig = new CacheEvictionConfig();

    private WanReplicationRef wanReplicationRef;
    private List<CachePartitionLostListenerConfig> partitionLostListenerConfigs;
    private String quorumName;
    private String mergePolicy = CacheSimpleConfig.DEFAULT_CACHE_MERGE_POLICY;

    public CacheConfig() {
    }

    public CacheConfig(String name) {
        setName(name);
    }

    public CacheConfig(CompleteConfiguration<K, V> configuration) {
        super(configuration);
        if (configuration instanceof CacheConfig) {
            final CacheConfig config = (CacheConfig) configuration;
            this.name = config.name;
            this.managerPrefix = config.managerPrefix;
            this.uriString = config.uriString;
            this.asyncBackupCount = config.asyncBackupCount;
            this.backupCount = config.backupCount;
            this.inMemoryFormat = config.inMemoryFormat;
            this.hotRestartConfig = new HotRestartConfig(config.hotRestartConfig);
            // Eviction config cannot be null
            if (config.evictionConfig != null) {
                this.evictionConfig = new CacheEvictionConfig(config.evictionConfig);
            }
            if (config.wanReplicationRef != null) {
                this.wanReplicationRef = new WanReplicationRef(config.wanReplicationRef);
            }
            if (config.partitionLostListenerConfigs != null) {
                this.partitionLostListenerConfigs = new ArrayList<CachePartitionLostListenerConfig>(
                        config.partitionLostListenerConfigs);
            }
            this.quorumName = config.quorumName;
            this.mergePolicy = config.mergePolicy;
        }
    }

    public CacheConfig(CacheSimpleConfig simpleConfig) throws Exception {
        this.name = simpleConfig.getName();
        if (simpleConfig.getKeyType() != null) {
            this.keyType = (Class<K>) ClassLoaderUtil.loadClass(null, simpleConfig.getKeyType());
        }
        if (simpleConfig.getValueType() != null) {
            this.valueType = (Class<V>) ClassLoaderUtil.loadClass(null, simpleConfig.getValueType());
        }
        this.isStatisticsEnabled = simpleConfig.isStatisticsEnabled();
        this.isManagementEnabled = simpleConfig.isManagementEnabled();
        this.isReadThrough = simpleConfig.isReadThrough();
        this.isWriteThrough = simpleConfig.isWriteThrough();
        if (simpleConfig.getCacheLoaderFactory() != null) {
            this.cacheLoaderFactory = ClassLoaderUtil.newInstance(null, simpleConfig.getCacheLoaderFactory());
        }
        if (simpleConfig.getCacheWriterFactory() != null) {
            this.cacheWriterFactory = ClassLoaderUtil.newInstance(null, simpleConfig.getCacheWriterFactory());
        }
        initExpiryPolicyFactoryConfig(simpleConfig);
        this.asyncBackupCount = simpleConfig.getAsyncBackupCount();
        this.backupCount = simpleConfig.getBackupCount();
        this.inMemoryFormat = simpleConfig.getInMemoryFormat();
        // Eviction config cannot be null
        if (simpleConfig.getEvictionConfig() != null) {
            this.evictionConfig = new CacheEvictionConfig(simpleConfig.getEvictionConfig());
        }
        if (simpleConfig.getWanReplicationRef() != null) {
            this.wanReplicationRef = new WanReplicationRef(simpleConfig.getWanReplicationRef());
        }
        for (CacheSimpleEntryListenerConfig simpleListener : simpleConfig.getCacheEntryListeners()) {
            Factory<? extends CacheEntryListener<? super K, ? super V>> listenerFactory = null;
            Factory<? extends CacheEntryEventFilter<? super K, ? super V>> filterFactory = null;
            if (simpleListener.getCacheEntryListenerFactory() != null) {
                listenerFactory = ClassLoaderUtil.newInstance(null, simpleListener.getCacheEntryListenerFactory());
            }
            if (simpleListener.getCacheEntryEventFilterFactory() != null) {
                filterFactory = ClassLoaderUtil.newInstance(null, simpleListener.getCacheEntryEventFilterFactory());
            }
            boolean isOldValueRequired = simpleListener.isOldValueRequired();
            boolean synchronous = simpleListener.isSynchronous();
            MutableCacheEntryListenerConfiguration<K, V> listenerConfiguration =
                    new MutableCacheEntryListenerConfiguration<K, V>(
                    listenerFactory, filterFactory, isOldValueRequired, synchronous);
            addCacheEntryListenerConfiguration(listenerConfiguration);
        }
        for (CachePartitionLostListenerConfig listenerConfig : simpleConfig.getPartitionLostListenerConfigs()) {
            getPartitionLostListenerConfigs().add(listenerConfig);
        }
        this.quorumName = simpleConfig.getQuorumName();
        this.mergePolicy = simpleConfig.getMergePolicy();
        this.hotRestartConfig = new HotRestartConfig(simpleConfig.getHotRestartConfig());
    }

    private void initExpiryPolicyFactoryConfig(CacheSimpleConfig simpleConfig) throws Exception {
        CacheSimpleConfig.ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                simpleConfig.getExpiryPolicyFactoryConfig();
        if (expiryPolicyFactoryConfig != null) {
            if (expiryPolicyFactoryConfig.getClassName() != null) {
                this.expiryPolicyFactory =
                        ClassLoaderUtil.newInstance(null, expiryPolicyFactoryConfig.getClassName());
            } else {
                TimedExpiryPolicyFactoryConfig timedExpiryPolicyConfig =
                        expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
                if (timedExpiryPolicyConfig != null) {
                    DurationConfig durationConfig = timedExpiryPolicyConfig.getDurationConfig();
                    ExpiryPolicyType expiryPolicyType = timedExpiryPolicyConfig.getExpiryPolicyType();
                    switch (expiryPolicyType) {
                        case CREATED:
                            this.expiryPolicyFactory =
                                    CreatedExpiryPolicy.factoryOf(
                                            new Duration(durationConfig.getTimeUnit(),
                                                         durationConfig.getDurationAmount()));
                            break;
                        case MODIFIED:
                            this.expiryPolicyFactory =
                                    ModifiedExpiryPolicy.factoryOf(
                                            new Duration(durationConfig.getTimeUnit(),
                                                         durationConfig.getDurationAmount()));
                            break;
                        case ACCESSED:
                            this.expiryPolicyFactory =
                                    AccessedExpiryPolicy.factoryOf(
                                            new Duration(durationConfig.getTimeUnit(),
                                                         durationConfig.getDurationAmount()));
                            break;
                        case TOUCHED:
                            this.expiryPolicyFactory =
                                    TouchedExpiryPolicy.factoryOf(
                                            new Duration(durationConfig.getTimeUnit(),
                                                         durationConfig.getDurationAmount()));
                            break;
                        case ETERNAL:
                            this.expiryPolicyFactory = EternalExpiryPolicy.factoryOf();
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported expiry policy type: " + expiryPolicyType);
                    }
                }
            }
        }
    }

    /**
     * Gets immutable version of this cache configuration.
     *
     * @return Immutable version of this cache configuration.
     */
    public CacheConfigReadOnly<K, V> getAsReadOnly() {
        return new CacheConfigReadOnly<K, V>(this);
    }

    /**
     * Gets the name of this {@link com.hazelcast.cache.ICache}.
     *
     * @return The name of this {@link com.hazelcast.cache.ICache}.
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
    public CacheConfig<K, V> setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the manager prefix of the {@link com.hazelcast.cache.ICache}, such as "hz://".
     *
     * @return The manager prefix of this {@link com.hazelcast.cache.ICache}.
     */
    public String getManagerPrefix() {
        return managerPrefix;
    }

    /**
     * Sets the manager prefix of the {@link com.hazelcast.cache.ICache}.
     *
     * @param managerPrefix The manager prefix to set for this {@link com.hazelcast.cache.ICache}.
     * @return The current cache config instance.
     */
    public CacheConfig<K, V> setManagerPrefix(String managerPrefix) {
        this.managerPrefix = managerPrefix;
        return this;
    }

    /**
     * Gets the URI string which is the global identifier for this {@link com.hazelcast.cache.ICache}.
     *
     * @return The URI string of this {@link com.hazelcast.cache.ICache}.
     */
    public String getUriString() {
        return uriString;
    }

    /**
     * Sets the URI string, which is the global identifier of the {@link com.hazelcast.cache.ICache}.
     *
     * @param uriString The URI string to set for this {@link com.hazelcast.cache.ICache}.
     * @return The current cache config instance.
     */
    public CacheConfig<K, V> setUriString(String uriString) {
        this.uriString = uriString;
        return this;
    }

    /**
     * Gets the full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix.
     *
     * @return The full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix.
     */
    public String getNameWithPrefix() {
        return managerPrefix + name;
    }

    /**
     * Gets the number of synchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @return The number of synchronous backups (backupCount) for this {@link com.hazelcast.cache.ICache}.
     * @see #getAsyncBackupCount()
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of synchronous backups. For example, if 1 is set as the backup count,
     * then all entries of the map will be copied to another JVM for
     * fail-safety. 0 means no synchronous backup.
     *
     * @param backupCount The number of synchronous backups to set for
     *                    this {@link com.hazelcast.cache.ICache}.
     * @return The current cache config instance.
     * @throws IllegalArgumentException if backupCount smaller than 0,
     *                                  or larger than the maximum number of backup,
     *                                  or the sum of the synchronous and asynchronous backups is larger than
     *                                  the maximum number of backups.
     * @see #setAsyncBackupCount(int)
     */
    public CacheConfig<K, V> setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Gets the number of asynchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @return the number of asynchronous backups for this {@link com.hazelcast.cache.ICache}.
     * @see #setBackupCount(int)
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @param asyncBackupCount The number of asynchronous backups to set
     *                         for this {@link com.hazelcast.cache.ICache}.
     * @return the updated CacheConfig
     * @throws new IllegalArgumentException if asyncBackupCount is smaller than 0,
     *             or larger than the maximum number of backups,
     *             or the sum of the synchronous and asynchronous backups is larger
     *             than the maximum number of backups.
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public CacheConfig<K, V> setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Gets the total backup count (<code>backupCount + asyncBackupCount</code>) of the cache.
     *
     * @return the total backup count (<code>backupCount + asyncBackupCount</code>) of the cache.
     */
    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * Gets the {@link EvictionConfig} instance of the eviction configuration for this {@link com.hazelcast.cache.ICache}.
     *
     * @return The {@link EvictionConfig} instance of the eviction configuration.
     */
    // TODO Change to "EvictionConfig" instead of "CacheEvictionConfig" in the future
    // since "CacheEvictionConfig" is deprecated
    public CacheEvictionConfig getEvictionConfig() {
        return evictionConfig;
    }

    /**
     * Sets the {@link EvictionConfig} instance for eviction configuration for this {@link com.hazelcast.cache.ICache}.
     *
     * @param evictionConfig The {@link EvictionConfig} instance to set for the eviction configuration.
     * @return The current cache config instance.
     */
    public CacheConfig setEvictionConfig(EvictionConfig evictionConfig) {
        isNotNull(evictionConfig, "Eviction config cannot be null !");

        // TODO Remove this check in the future since "CacheEvictionConfig" is deprecated
        if (evictionConfig instanceof CacheEvictionConfig) {
            this.evictionConfig = (CacheEvictionConfig) evictionConfig;
        } else {
            this.evictionConfig = new CacheEvictionConfig(evictionConfig);
        }

        return this;
    }

    public WanReplicationRef getWanReplicationRef() {
        return wanReplicationRef;
    }

    public CacheConfig setWanReplicationRef(WanReplicationRef wanReplicationRef) {
        this.wanReplicationRef = wanReplicationRef;
        return this;
    }

    /**
     * Gets the partition lost listener references added to cache configuration.
     *
     * @return List of CachePartitionLostListenerConfig.
     */
    public List<CachePartitionLostListenerConfig> getPartitionLostListenerConfigs() {
        if (partitionLostListenerConfigs == null) {
            partitionLostListenerConfigs = new ArrayList<CachePartitionLostListenerConfig>();
        }
        return partitionLostListenerConfigs;
    }

    /**
     * Sets the WAN target replication reference.
     *
     * @param partitionLostListenerConfigs CachePartitionLostListenerConfig list.
     */
    public CacheConfig setPartitionLostListenerConfigs(List<CachePartitionLostListenerConfig> partitionLostListenerConfigs) {
        this.partitionLostListenerConfigs = partitionLostListenerConfigs;
        return this;
    }

    /**
     * Gets the data type that will be used to store records.
     *
     * @return the data storage type of the cache config.
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Data type that will be used to store records in this {@link com.hazelcast.cache.ICache}.
     * Possible values:
     * BINARY (default): keys and values will be stored as binary data.
     * OBJECT: values will be stored in their object forms.
     *
     * @param inMemoryFormat the record type to set.
     * @return current cache config instance.
     * @throws IllegalArgumentException if inMemoryFormat is null.
     */
    public CacheConfig<K, V> setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat, "In-Memory format cannot be null !");
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
     * @param quorumName name of the desired quorum.
     *
     * @return the updated CacheConfig.
     */
    public CacheConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }

    /**
     * Gets the class name of {@link com.hazelcast.cache.CacheMergePolicy}
     * implementation of this cache config.
     *
     * @return the class name of {@link com.hazelcast.cache.CacheMergePolicy}
     *         implementation of this cache config
     */
    public String getMergePolicy() {
        return mergePolicy;
    }

    /**
     * Sets the class name of {@link com.hazelcast.cache.CacheMergePolicy}
     * implementation to this cache config.
     *
     * @param mergePolicy the class name of {@link com.hazelcast.cache.CacheMergePolicy}
     *                    implementation to be set to this cache config
     */
    public void setMergePolicy(String mergePolicy) {
        this.mergePolicy = mergePolicy;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeUTF(managerPrefix);
        out.writeUTF(uriString);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);

        out.writeUTF(inMemoryFormat.name());
        out.writeObject(evictionConfig);

        out.writeObject(wanReplicationRef);
        //SUPER
        out.writeObject(keyType);
        out.writeObject(valueType);
        out.writeObject(cacheLoaderFactory);
        out.writeObject(cacheWriterFactory);
        out.writeObject(expiryPolicyFactory);

        out.writeBoolean(isReadThrough);
        out.writeBoolean(isWriteThrough);
        out.writeBoolean(isStoreByValue);
        out.writeBoolean(isManagementEnabled);
        out.writeBoolean(isStatisticsEnabled);
        out.writeBoolean(hotRestartConfig.isEnabled());
        out.writeBoolean(hotRestartConfig.isFsync());

        out.writeUTF(quorumName);

        final boolean listNotEmpty = listenerConfigurations != null && !listenerConfigurations.isEmpty();
        out.writeBoolean(listNotEmpty);
        if (listNotEmpty) {
            out.writeInt(listenerConfigurations.size());
            for (CacheEntryListenerConfiguration<K, V> cc : listenerConfigurations) {
                out.writeObject(cc);
            }
        }

        out.writeUTF(mergePolicy);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        managerPrefix = in.readUTF();
        uriString = in.readUTF();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();

        String resultInMemoryFormat = in.readUTF();
        inMemoryFormat = InMemoryFormat.valueOf(resultInMemoryFormat);
        evictionConfig = in.readObject();

        wanReplicationRef = in.readObject();

        //SUPER
        keyType = in.readObject();
        valueType = in.readObject();
        cacheLoaderFactory = in.readObject();
        cacheWriterFactory = in.readObject();
        expiryPolicyFactory = in.readObject();

        isReadThrough = in.readBoolean();
        isWriteThrough = in.readBoolean();
        isStoreByValue = in.readBoolean();
        isManagementEnabled = in.readBoolean();
        isStatisticsEnabled = in.readBoolean();
        hotRestartConfig.setEnabled(in.readBoolean());
        hotRestartConfig.setFsync(in.readBoolean());

        quorumName = in.readUTF();

        final boolean listNotEmpty = in.readBoolean();
        if (listNotEmpty) {
            final int size = in.readInt();
            listenerConfigurations = createConcurrentSet();
            for (int i = 0; i < size; i++) {
                listenerConfigurations.add((CacheEntryListenerConfiguration<K, V>) in.readObject());
            }
        }

        mergePolicy = in.readUTF();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (managerPrefix != null ? managerPrefix.hashCode() : 0);
        result = 31 * result + (uriString != null ? uriString.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof CacheConfig)) {
            return false;
        }

        final CacheConfig that = (CacheConfig) o;


        if (managerPrefix != null ? !managerPrefix.equals(that.managerPrefix) : that.managerPrefix != null) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (uriString != null ? !uriString.equals(that.uriString) : that.uriString != null) {
            return false;
        }

        return super.equals(o);
    }

    @Override
    public String toString() {
        return "CacheConfig{"
                + "name='" + name + '\''
                + ", managerPrefix='" + managerPrefix + '\''
                + ", inMemoryFormat=" + inMemoryFormat
                + ", backupCount=" + backupCount
                + ", hotRestart=" + hotRestartConfig
                + '}';
    }
}
