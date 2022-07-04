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

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.DeferredValue;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import javax.cache.integration.CacheWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.config.CacheSimpleConfig.DEFAULT_BACKUP_COUNT;
import static com.hazelcast.config.CacheSimpleConfig.DEFAULT_IN_MEMORY_FORMAT;
import static com.hazelcast.config.CacheSimpleConfig.MIN_BACKUP_COUNT;
import static com.hazelcast.internal.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Contains all the configuration for the {@link com.hazelcast.cache.ICache}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class CacheConfig<K, V> extends AbstractCacheConfig<K, V> implements Versioned {

    private String name;
    private String managerPrefix;
    private String uriString;
    private int asyncBackupCount = MIN_BACKUP_COUNT;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private String splitBrainProtectionName;
    private WanReplicationRef wanReplicationRef;
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    // Default value of eviction config is
    //      * ENTRY_COUNT with 10000 max entry count
    //      * LRU as eviction policy
    private EvictionConfig evictionConfig = new EvictionConfig();
    @SuppressFBWarnings("SE_BAD_FIELD")
    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
    private MerkleTreeConfig merkleTreeConfig = new MerkleTreeConfig();
    private List<CachePartitionLostListenerConfig> partitionLostListenerConfigs;

    /**
     * Disables invalidation events for per entry but full-flush invalidation events are still enabled.
     * Full-flush invalidation means the invalidation of events for all entries when clear is called.
     */
    private boolean disablePerEntryInvalidationEvents;

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
            this.dataPersistenceConfig = new DataPersistenceConfig(config.dataPersistenceConfig);
            this.eventJournalConfig = new EventJournalConfig(config.eventJournalConfig);
            // eviction config is not allowed to be null
            if (config.evictionConfig != null) {
                this.evictionConfig = new EvictionConfig(config.evictionConfig);
            }
            if (config.wanReplicationRef != null) {
                this.wanReplicationRef = new WanReplicationRef(config.wanReplicationRef);
            }
            if (config.partitionLostListenerConfigs != null) {
                this.partitionLostListenerConfigs = new ArrayList<CachePartitionLostListenerConfig>(
                        config.partitionLostListenerConfigs);
            }
            this.splitBrainProtectionName = config.splitBrainProtectionName;
            this.mergePolicyConfig = new MergePolicyConfig(config.mergePolicyConfig);
            this.merkleTreeConfig = new MerkleTreeConfig(config.merkleTreeConfig);
            this.disablePerEntryInvalidationEvents = config.disablePerEntryInvalidationEvents;
            this.serializationService = config.serializationService;
            this.classLoader = config.classLoader;
        }
    }

    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public CacheConfig(CacheSimpleConfig simpleConfig) throws Exception {
        this.name = simpleConfig.getName();
        if (simpleConfig.getKeyType() != null) {
            setKeyClassName(simpleConfig.getKeyType());
        }
        if (simpleConfig.getValueType() != null) {
            setValueClassName(simpleConfig.getValueType());
        }
        this.isStatisticsEnabled = simpleConfig.isStatisticsEnabled();
        this.isManagementEnabled = simpleConfig.isManagementEnabled();
        this.isReadThrough = simpleConfig.isReadThrough();
        this.isWriteThrough = simpleConfig.isWriteThrough();
        copyFactories(simpleConfig);
        initExpiryPolicyFactoryConfig(simpleConfig);
        this.asyncBackupCount = simpleConfig.getAsyncBackupCount();
        this.backupCount = simpleConfig.getBackupCount();
        this.inMemoryFormat = simpleConfig.getInMemoryFormat();
        // eviction config is not allowed to be null
        if (simpleConfig.getEvictionConfig() != null) {
            this.evictionConfig = new EvictionConfig(simpleConfig.getEvictionConfig());
        }
        if (simpleConfig.getWanReplicationRef() != null) {
            this.wanReplicationRef = new WanReplicationRef(simpleConfig.getWanReplicationRef());
        }
        copyListeners(simpleConfig);
        this.splitBrainProtectionName = simpleConfig.getSplitBrainProtectionName();
        this.mergePolicyConfig = new MergePolicyConfig(simpleConfig.getMergePolicyConfig());
        this.merkleTreeConfig = new MerkleTreeConfig(simpleConfig.getMerkleTreeConfig());
        this.hotRestartConfig = new HotRestartConfig(simpleConfig.getHotRestartConfig());
        this.dataPersistenceConfig = new DataPersistenceConfig(simpleConfig.getDataPersistenceConfig());
        this.eventJournalConfig = new EventJournalConfig(simpleConfig.getEventJournalConfig());
        this.disablePerEntryInvalidationEvents = simpleConfig.isDisablePerEntryInvalidationEvents();
    }

    private void initExpiryPolicyFactoryConfig(CacheSimpleConfig simpleConfig) throws Exception {
        CacheSimpleConfig.ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                simpleConfig.getExpiryPolicyFactoryConfig();
        if (expiryPolicyFactoryConfig != null) {
            if (expiryPolicyFactoryConfig.getClassName() != null) {
                setExpiryPolicyFactory(
                        ClassLoaderUtil.newInstance(
                                null,
                                expiryPolicyFactoryConfig.getClassName()
                        )
                );
            } else {
                TimedExpiryPolicyFactoryConfig timedExpiryPolicyConfig =
                        expiryPolicyFactoryConfig.getTimedExpiryPolicyFactoryConfig();
                if (timedExpiryPolicyConfig != null) {
                    DurationConfig durationConfig = timedExpiryPolicyConfig.getDurationConfig();
                    ExpiryPolicyType expiryPolicyType = timedExpiryPolicyConfig.getExpiryPolicyType();
                    switch (expiryPolicyType) {
                        case CREATED:
                            setExpiryPolicyFactory(
                                    CreatedExpiryPolicy.factoryOf(
                                            new Duration(durationConfig.getTimeUnit(),
                                                    durationConfig.getDurationAmount())));
                            break;
                        case MODIFIED:
                            setExpiryPolicyFactory(
                                    ModifiedExpiryPolicy.factoryOf(
                                            new Duration(durationConfig.getTimeUnit(),
                                                    durationConfig.getDurationAmount())));
                            break;
                        case ACCESSED:
                            setExpiryPolicyFactory(
                                    AccessedExpiryPolicy.factoryOf(
                                            new Duration(durationConfig.getTimeUnit(),
                                                    durationConfig.getDurationAmount())));
                            break;
                        case TOUCHED:
                            setExpiryPolicyFactory(
                                    TouchedExpiryPolicy.factoryOf(
                                            new Duration(durationConfig.getTimeUnit(),
                                                    durationConfig.getDurationAmount())));
                            break;
                        case ETERNAL:
                            setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf());
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported expiry policy type: " + expiryPolicyType);
                    }
                }
            }
        }
    }

    /**
     * Gets the name of this {@link com.hazelcast.cache.ICache}.
     *
     * @return the name of this {@link com.hazelcast.cache.ICache}
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
    public CacheConfig<K, V> setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the manager prefix of the {@link com.hazelcast.cache.ICache}, such as "hz://".
     *
     * @return the manager prefix of this {@link com.hazelcast.cache.ICache}
     */
    public String getManagerPrefix() {
        return managerPrefix;
    }

    /**
     * Sets the manager prefix of the {@link com.hazelcast.cache.ICache}.
     *
     * @param managerPrefix the manager prefix to set for this {@link com.hazelcast.cache.ICache}
     * @return the current cache config instance
     */
    public CacheConfig<K, V> setManagerPrefix(String managerPrefix) {
        this.managerPrefix = managerPrefix;
        return this;
    }

    /**
     * Gets the URI string which is the global identifier for this {@link com.hazelcast.cache.ICache}.
     *
     * @return the URI string of this {@link com.hazelcast.cache.ICache}
     */
    public String getUriString() {
        return uriString;
    }

    /**
     * Sets the URI string, which is the global identifier of the {@link com.hazelcast.cache.ICache}.
     *
     * @param uriString the URI string to set for this {@link com.hazelcast.cache.ICache}
     * @return the current cache config instance
     */
    public CacheConfig<K, V> setUriString(String uriString) {
        this.uriString = uriString;
        return this;
    }

    /**
     * Gets the full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix.
     *
     * @return the full name of the {@link com.hazelcast.cache.ICache}, including the manager scope prefix
     */
    public String getNameWithPrefix() {
        return managerPrefix + name;
    }

    /**
     * Gets the number of synchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @return the number of synchronous backups (backupCount) for this {@link com.hazelcast.cache.ICache}
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
     * @param backupCount the number of synchronous backups to set for this {@link com.hazelcast.cache.ICache}
     * @return the current cache config instance
     * @throws IllegalArgumentException if backupCount smaller than 0,
     *                                  or larger than the maximum number of backup,
     *                                  or the sum of the synchronous and asynchronous backups is larger than
     *                                  the maximum number of backups
     * @see #setAsyncBackupCount(int)
     */
    public CacheConfig<K, V> setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Gets the number of asynchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @return the number of asynchronous backups for this {@link com.hazelcast.cache.ICache}
     * @see #setBackupCount(int)
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups for this {@link com.hazelcast.cache.ICache}.
     *
     * @param asyncBackupCount the number of asynchronous backups to set for this {@link com.hazelcast.cache.ICache}
     * @return the updated CacheConfig
     * @throws IllegalArgumentException if asyncBackupCount is smaller than 0,
     *                                  or larger than the maximum number of backups,
     *                                  or the sum of the synchronous and asynchronous backups is larger
     *                                  than the maximum number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public CacheConfig<K, V> setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Gets the total backup count ({@code backupCount + asyncBackupCount}) of the cache.
     *
     * @return the total backup count ({@code backupCount + asyncBackupCount}) of the cache
     */
    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * Gets the {@link EvictionConfig} instance of the eviction configuration for this {@link com.hazelcast.cache.ICache}.
     *
     * @return the {@link EvictionConfig} instance of the eviction configuration
     */
    public EvictionConfig getEvictionConfig() {
        return evictionConfig;
    }

    /**
     * Sets the {@link EvictionConfig} instance for eviction configuration for this {@link com.hazelcast.cache.ICache}.
     *
     * @param evictionConfig the {@link EvictionConfig} instance to set for the eviction configuration
     * @return the current cache config instance
     */
    public CacheConfig<K, V> setEvictionConfig(EvictionConfig evictionConfig) {
        isNotNull(evictionConfig, "evictionConfig");
        this.evictionConfig = evictionConfig;
        return this;
    }

    public WanReplicationRef getWanReplicationRef() {
        return wanReplicationRef;
    }

    public CacheConfig<K, V> setWanReplicationRef(WanReplicationRef wanReplicationRef) {
        this.wanReplicationRef = wanReplicationRef;
        return this;
    }

    /**
     * Gets the partition lost listener references added to cache configuration.
     *
     * @return List of CachePartitionLostListenerConfig
     */
    public List<CachePartitionLostListenerConfig> getPartitionLostListenerConfigs() {
        if (partitionLostListenerConfigs == null) {
            partitionLostListenerConfigs = new ArrayList<>();
        }
        return partitionLostListenerConfigs;
    }

    /**
     * Sets the WAN target replication reference.
     *
     * @param partitionLostListenerConfigs CachePartitionLostListenerConfig list
     */
    public CacheConfig<K, V> setPartitionLostListenerConfigs(
            List<CachePartitionLostListenerConfig> partitionLostListenerConfigs) {
        this.partitionLostListenerConfigs = partitionLostListenerConfigs;
        return this;
    }

    /**
     * Gets the data type that will be used to store records.
     *
     * @return the data storage type of the cache config
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Data type that will be used to store records in this {@link com.hazelcast.cache.ICache}.
     * Possible values:
     * <ul>
     * <li>BINARY (default): keys and values will be stored as binary data</li>
     * <li>OBJECT: values will be stored in their object forms</li>
     * <li>NATIVE: values will be stored in non-heap region of JVM (Hazelcast Enterprise only)</li>
     * </ul>
     *
     * @param inMemoryFormat the record type to set
     * @return current cache config instance
     * @throws IllegalArgumentException if inMemoryFormat is {@code null}
     */
    public CacheConfig<K, V> setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat, "In-Memory format cannot be null!");
        return this;
    }

    /**
     * Gets the name of the associated split brain protection if any.
     *
     * @return the name of the associated split brain protection if any
     */
    public String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    /**
     * Associates this cache configuration to a split brain protection.
     *
     * @param splitBrainProtectionName name of the desired split brain protection
     * @return the updated CacheConfig
     */
    public CacheConfig<K, V> setSplitBrainProtectionName(String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
        return this;
    }

    /**
     * Gets the {@link MergePolicyConfig} for this map.
     *
     * @return the {@link MergePolicyConfig} for this map
     */
    public MergePolicyConfig getMergePolicyConfig() {
        return mergePolicyConfig;
    }

    /**
     * Sets the {@link MergePolicyConfig} for this map.
     *
     * @return the updated map configuration
     */
    public CacheConfig<K, V> setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = checkNotNull(mergePolicyConfig, "mergePolicyConfig cannot be null!");
        return this;
    }

    /**
     * Gets the {@code MerkleTreeConfig} for this {@code CacheConfig}
     *
     * @return merkle tree config
     */
    public MerkleTreeConfig getMerkleTreeConfig() {
        return merkleTreeConfig;
    }

    /**
     * Sets the {@code MerkleTreeConfig} for this {@code CacheConfig}
     *
     * @param merkleTreeConfig merkle tree config
     */
    public void setMerkleTreeConfig(MerkleTreeConfig merkleTreeConfig) {
        this.merkleTreeConfig = checkNotNull(merkleTreeConfig, "merkleTreeConfig cannot be null!");
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
     * @param disablePerEntryInvalidationEvents disables invalidation event sending behaviour if it is {@code true},
     *                                          otherwise enables it
     * @return this configuration
     */
    public CacheConfig<K, V> setDisablePerEntryInvalidationEvents(boolean disablePerEntryInvalidationEvents) {
        this.disablePerEntryInvalidationEvents = disablePerEntryInvalidationEvents;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(managerPrefix);
        out.writeString(uriString);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);

        out.writeString(inMemoryFormat.name());
        out.writeObject(evictionConfig);

        out.writeObject(wanReplicationRef);
        // SUPER
        writeKeyValueTypes(out);
        writeFactories(out);

        out.writeBoolean(isReadThrough);
        out.writeBoolean(isWriteThrough);
        out.writeBoolean(isStoreByValue);
        out.writeBoolean(isManagementEnabled);
        out.writeBoolean(isStatisticsEnabled);
        out.writeObject(hotRestartConfig);
        out.writeObject(eventJournalConfig);

        out.writeString(splitBrainProtectionName);

        out.writeBoolean(hasListenerConfiguration());
        if (hasListenerConfiguration()) {
            writeListenerConfigurations(out);
        }

        out.writeObject(mergePolicyConfig);
        out.writeBoolean(disablePerEntryInvalidationEvents);

        writePartitionLostListenerConfigs(out);

        out.writeObject(merkleTreeConfig);
        out.writeObject(dataPersistenceConfig);
    }

    private void writePartitionLostListenerConfigs(ObjectDataOutput out)
            throws IOException {
        if (partitionLostListenerConfigs == null) {
            out.writeInt(Bits.NULL_ARRAY_LENGTH);
            return;
        }

        out.writeInt(partitionLostListenerConfigs.size());
        for (CachePartitionLostListenerConfig partitionLostListenerConfig : partitionLostListenerConfigs) {
            out.writeObject(partitionLostListenerConfig);
        }
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.CACHE_CONFIG;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        managerPrefix = in.readString();
        uriString = in.readString();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();

        String resultInMemoryFormat = in.readString();
        inMemoryFormat = InMemoryFormat.valueOf(resultInMemoryFormat);
        evictionConfig = in.readObject();
        wanReplicationRef = in.readObject();

        readKeyValueTypes(in);
        readFactories(in);

        isReadThrough = in.readBoolean();
        isWriteThrough = in.readBoolean();
        isStoreByValue = in.readBoolean();
        isManagementEnabled = in.readBoolean();
        isStatisticsEnabled = in.readBoolean();
        setHotRestartConfig(in.readObject());
        eventJournalConfig = in.readObject();

        splitBrainProtectionName = in.readString();

        final boolean listNotEmpty = in.readBoolean();
        if (listNotEmpty) {
            readListenerConfigurations(in);
        }

        mergePolicyConfig = in.readObject();
        disablePerEntryInvalidationEvents = in.readBoolean();

        setClassLoader(in.getClassLoader());
        assert in instanceof SerializationServiceSupport;
        this.serializationService = ((SerializationServiceSupport) in).getSerializationService();

        readPartitionLostListenerConfigs(in);

        merkleTreeConfig = in.readObject();
        setDataPersistenceConfig(in.readObject());
    }

    private void readPartitionLostListenerConfigs(ObjectDataInput in)
            throws IOException {
        int partitionLostListenerConfigCount = in.readInt();
        if (partitionLostListenerConfigCount > 0) {
            partitionLostListenerConfigs = new ArrayList<>(partitionLostListenerConfigCount);
            for (int i = 0; i < partitionLostListenerConfigCount; i++) {
                partitionLostListenerConfigs.add(in.readObject());
            }
        }
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
        if (!Objects.equals(managerPrefix, that.managerPrefix)) {
            return false;
        }
        if (!Objects.equals(name, that.name)) {
            return false;
        }
        if (!Objects.equals(uriString, that.uriString)) {
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
                + ", dataPersistenceConfig=" + dataPersistenceConfig
                + ", wanReplicationRef=" + wanReplicationRef
                + ", merkleTreeConfig=" + merkleTreeConfig
                + '}';
    }

    protected void writeTenant(ObjectDataOutput out) throws IOException {
    }

    protected void readTenant(ObjectDataInput in) throws IOException {
    }

    protected void writeKeyValueTypes(ObjectDataOutput out) throws IOException {
        out.writeObject(getKeyType());
        out.writeObject(getValueType());
    }

    protected void readKeyValueTypes(ObjectDataInput in) throws IOException {
        setKeyType(in.readObject());
        setValueType(in.readObject());
    }

    protected void writeFactories(ObjectDataOutput out) throws IOException {
        out.writeObject(getCacheLoaderFactory());
        out.writeObject(getCacheWriterFactory());
        out.writeObject(getExpiryPolicyFactory());
    }

    protected void readFactories(ObjectDataInput in) throws IOException {
        setCacheLoaderFactory(in.readObject());
        setCacheWriterFactory(in.readObject());
        setExpiryPolicyFactory(in.readObject());
    }

    protected void writeListenerConfigurations(ObjectDataOutput out) throws IOException {
        out.writeInt(getListenerConfigurations().size());
        for (CacheEntryListenerConfiguration<K, V> cc : getListenerConfigurations()) {
            out.writeObject(cc);
        }
    }

    protected void readListenerConfigurations(ObjectDataInput in) throws IOException {
        final int size = in.readInt();
        Set<DeferredValue<CacheEntryListenerConfiguration<K, V>>> lc = createConcurrentSet();
        for (int i = 0; i < size; i++) {
            lc.add(DeferredValue.withValue(in.readObject()));
        }
        listenerConfigurations = lc;
    }

    /**
     * Copy this CacheConfig to given {@code target} object whose type extends CacheConfig.
     *
     * @param target   the target object to which this configuration will be copied
     * @param resolved when {@code true}, it is assumed that this {@code cacheConfig}'s key-value types have already been
     *                 or will be resolved to loaded classes and the actual {@code keyType} and {@code valueType} will be copied.
     *                 Otherwise, this configuration's {@code keyClassName} and {@code valueClassName} will be copied to the
     *                 target config, to be resolved at a later time.
     * @param <T>      the target object type
     * @return the target config
     */
    public <T extends CacheConfig<K, V>> T copy(T target, boolean resolved) {
        target.setAsyncBackupCount(getAsyncBackupCount());
        target.setBackupCount(getBackupCount());
        target.setDisablePerEntryInvalidationEvents(isDisablePerEntryInvalidationEvents());
        target.setEvictionConfig(getEvictionConfig());
        target.setHotRestartConfig(getHotRestartConfig());
        target.setEventJournalConfig(getEventJournalConfig());
        target.setInMemoryFormat(getInMemoryFormat());
        if (resolved) {
            target.setKeyType(getKeyType());
            target.setValueType(getValueType());
        } else {
            target.setKeyClassName(getKeyClassName());
            target.setValueClassName(getValueClassName());
        }

        target.cacheLoaderFactory = cacheLoaderFactory.shallowCopy(resolved, serializationService);
        target.cacheWriterFactory = cacheWriterFactory.shallowCopy(resolved, serializationService);
        target.expiryPolicyFactory = expiryPolicyFactory.shallowCopy(resolved, serializationService);

        target.listenerConfigurations = createConcurrentSet();
        for (DeferredValue<CacheEntryListenerConfiguration<K, V>> lazyEntryListenerConfig : listenerConfigurations) {
            target.listenerConfigurations.add(lazyEntryListenerConfig.shallowCopy(resolved, serializationService));
        }

        target.setManagementEnabled(isManagementEnabled());
        target.setManagerPrefix(getManagerPrefix());
        target.setMergePolicyConfig(getMergePolicyConfig());
        target.setMerkleTreeConfig(getMerkleTreeConfig());
        target.setName(getName());
        target.setPartitionLostListenerConfigs(getPartitionLostListenerConfigs());
        target.setSplitBrainProtectionName(getSplitBrainProtectionName());
        target.setReadThrough(isReadThrough());
        target.setStatisticsEnabled(isStatisticsEnabled());
        target.setStoreByValue(isStoreByValue());
        target.setUriString(getUriString());
        target.setWanReplicationRef(getWanReplicationRef());
        target.setWriteThrough(isWriteThrough());
        target.setClassLoader(classLoader);
        target.serializationService = serializationService;
        return target;
    }

    private void copyListeners(CacheSimpleConfig simpleConfig)
            throws Exception {
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
            MutableCacheEntryListenerConfiguration<K, V> listenerConfiguration = new MutableCacheEntryListenerConfiguration<>(
                    listenerFactory, filterFactory, isOldValueRequired, synchronous);
            addCacheEntryListenerConfiguration(listenerConfiguration);
        }
        for (CachePartitionLostListenerConfig listenerConfig : simpleConfig.getPartitionLostListenerConfigs()) {
            getPartitionLostListenerConfigs().add(listenerConfig);
        }
    }

    private void copyFactories(CacheSimpleConfig simpleConfig) throws Exception {
        if (simpleConfig.getCacheLoaderFactory() != null) {
            setCacheLoaderFactory(
                    ClassLoaderUtil.newInstance(
                            null,
                            simpleConfig.getCacheLoaderFactory()
                    )
            );
        }
        if (simpleConfig.getCacheLoader() != null) {
            setCacheLoaderFactory(FactoryBuilder.factoryOf(simpleConfig.getCacheLoader()));
        }
        if (simpleConfig.getCacheWriterFactory() != null) {
            setCacheWriterFactory(
                    ClassLoaderUtil.<Factory<? extends CacheWriter<K, V>>>newInstance(
                            null,
                            simpleConfig.getCacheWriterFactory()
                    )
            );
        }
        if (simpleConfig.getCacheWriter() != null) {
            setCacheWriterFactory(FactoryBuilder.<CacheWriter<K, V>>factoryOf(simpleConfig.getCacheWriter()));
        }
    }
}
