/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.DeferredValue;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.spi.merge.SplitBrainMergeTypeProvider;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;
import com.hazelcast.spi.tenantcontrol.TenantControl;

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
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.hazelcast.config.CacheSimpleConfig.DEFAULT_BACKUP_COUNT;
import static com.hazelcast.config.CacheSimpleConfig.DEFAULT_IN_MEMORY_FORMAT;
import static com.hazelcast.config.CacheSimpleConfig.MIN_BACKUP_COUNT;
import static com.hazelcast.spi.tenantcontrol.TenantControl.NOOP_TENANT_CONTROL;
import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains all the configuration for the {@link com.hazelcast.cache.ICache}.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@BinaryInterface
public class CacheConfig<K, V> extends AbstractCacheConfig<K, V> implements SplitBrainMergeTypeProvider {

    private String name;
    private String managerPrefix;
    private String uriString;
    private int asyncBackupCount = MIN_BACKUP_COUNT;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    // Default value of eviction config is
    //      * ENTRY_COUNT with 10000 max entry count
    //      * LRU as eviction policy
    // TODO: change to "EvictionConfig" in the future since "CacheEvictionConfig" is deprecated
    private CacheEvictionConfig evictionConfig = new CacheEvictionConfig();

    private WanReplicationRef wanReplicationRef;
    private List<CachePartitionLostListenerConfig> partitionLostListenerConfigs;
    private String quorumName;
    private String mergePolicy = CacheSimpleConfig.DEFAULT_CACHE_MERGE_POLICY;

    /**
     * Disables invalidation events for per entry but full-flush invalidation events are still enabled.
     * Full-flush invalidation means the invalidation of events for all entries when clear is called.
     */
    private boolean disablePerEntryInvalidationEvents;

    private TenantControl tenantControl = NOOP_TENANT_CONTROL;

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
            // eviction config is not allowed to be null
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
            this.evictionConfig = new CacheEvictionConfig(simpleConfig.getEvictionConfig());
        }
        if (simpleConfig.getWanReplicationRef() != null) {
            this.wanReplicationRef = new WanReplicationRef(simpleConfig.getWanReplicationRef());
        }
        copyListeners(simpleConfig);
        this.quorumName = simpleConfig.getQuorumName();
        this.mergePolicy = simpleConfig.getMergePolicy();
        this.hotRestartConfig = new HotRestartConfig(simpleConfig.getHotRestartConfig());
        this.disablePerEntryInvalidationEvents = simpleConfig.isDisablePerEntryInvalidationEvents();
    }

    private void initExpiryPolicyFactoryConfig(CacheSimpleConfig simpleConfig) throws Exception {
        CacheSimpleConfig.ExpiryPolicyFactoryConfig expiryPolicyFactoryConfig =
                simpleConfig.getExpiryPolicyFactoryConfig();
        if (expiryPolicyFactoryConfig != null) {
            if (expiryPolicyFactoryConfig.getClassName() != null) {
                setExpiryPolicyFactory(
                        ClassLoaderUtil.<Factory<? extends ExpiryPolicy>>newInstance(
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
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public CacheConfigReadOnly<K, V> getAsReadOnly() {
        return new CacheConfigReadOnly<K, V>(this);
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
    // TODO: change to "EvictionConfig" in the future since "CacheEvictionConfig" is deprecated
    public CacheEvictionConfig getEvictionConfig() {
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

        // TODO: remove this check in the future since "CacheEvictionConfig" is deprecated
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
            partitionLostListenerConfigs = new ArrayList<CachePartitionLostListenerConfig>();
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
     * @return the updated CacheConfig
     */
    public CacheConfig<K, V> setQuorumName(String quorumName) {
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
     * @param mergePolicy the class name of {@link com.hazelcast.cache.CacheMergePolicy} implementation to be set to this cache
     *                    config
     */
    public void setMergePolicy(String mergePolicy) {
        this.mergePolicy = mergePolicy;
    }

    @Override
    public Class getProvidedMergeTypes() {
        return SplitBrainMergeTypes.CacheMergeTypes.class;
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
     */
    public void setDisablePerEntryInvalidationEvents(boolean disablePerEntryInvalidationEvents) {
        this.disablePerEntryInvalidationEvents = disablePerEntryInvalidationEvents;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(managerPrefix);
        out.writeUTF(uriString);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);

        out.writeUTF(inMemoryFormat.name());
        out.writeObject(evictionConfig);

        out.writeObject(wanReplicationRef);
        // SUPER
        writeKeyValueTypes(out);
        writeTenant(out);
        writeFactories(out);

        out.writeBoolean(isReadThrough);
        out.writeBoolean(isWriteThrough);
        out.writeBoolean(isStoreByValue);
        out.writeBoolean(isManagementEnabled);
        out.writeBoolean(isStatisticsEnabled);
        out.writeBoolean(hotRestartConfig.isEnabled());
        out.writeBoolean(hotRestartConfig.isFsync());

        out.writeUTF(quorumName);

        out.writeBoolean(hasListenerConfiguration());
        if (hasListenerConfiguration()) {
            writeListenerConfigurations(out);
        }

        out.writeUTF(mergePolicy);
        out.writeBoolean(disablePerEntryInvalidationEvents);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        managerPrefix = in.readUTF();
        uriString = in.readUTF();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();

        String resultInMemoryFormat = in.readUTF();
        inMemoryFormat = InMemoryFormat.valueOf(resultInMemoryFormat);
        // set the thread-context and class loading context for this cache's tenant application
        // This way user customizations (loader factories, listeners) and keyType/valueType
        // can be CDI / EJB / JPA objects
        Closeable tenantContext = tenantControl.setTenant(false);
        try {
            evictionConfig = in.readObject();
            wanReplicationRef = in.readObject();

            readKeyValueTypes(in);
            readTenant(in);
            readFactories(in);

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
                readListenerConfigurations(in);
            }
        } finally {
            tenantContext.close();
        }

        mergePolicy = in.readUTF();
        disablePerEntryInvalidationEvents = in.readBoolean();

        setClassLoader(in.getClassLoader());
        this.serializationService = in.getSerializationService();
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
                + ", wanReplicationRef=" + wanReplicationRef
                + '}';
    }

    TenantControl getTenantControl() {
        return tenantControl;
    }

    void setTenantControl(TenantControl tenantControl) {
        this.tenantControl = tenantControl;
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
        setKeyType((Class<K>) in.readObject());
        setValueType((Class<V>) in.readObject());
    }

    protected void writeFactories(ObjectDataOutput out) throws IOException {
        out.writeObject(getCacheLoaderFactory());
        out.writeObject(getCacheWriterFactory());
        out.writeObject(getExpiryPolicyFactory());
    }

    protected void readFactories(ObjectDataInput in) throws IOException {
        setCacheLoaderFactory(in.<Factory<? extends CacheLoader<K, V>>>readObject());
        setCacheWriterFactory(in.<Factory<? extends CacheWriter<? super K, ? super V>>>readObject());
        setExpiryPolicyFactory(in.<Factory<? extends ExpiryPolicy>>readObject());
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
            lc.add(DeferredValue.withValue((CacheEntryListenerConfiguration<K, V>) in.readObject()));
        }
        listenerConfigurations = lc;
    }

    /**
     * Copy this CacheConfig to given {@code target} object whose type extends CacheConfig.
     *
     * @param target    the target object to which this configuration will be copied
     * @param resolved  when {@code true}, it is assumed that this {@code cacheConfig}'s key-value types have already been
     *                  or will be resolved to loaded classes and the actual {@code keyType} and {@code valueType} will be copied.
     *                  Otherwise, this configuration's {@code keyClassName} and {@code valueClassName} will be copied to the
     *                  target config, to be resolved at a later time.
     * @return          the target config
     */
    public <T extends CacheConfig<K, V>> T copy(T target, boolean resolved) {
        target.setTenantControl(getTenantControl());
        target.setAsyncBackupCount(getAsyncBackupCount());
        target.setBackupCount(getBackupCount());
        target.setDisablePerEntryInvalidationEvents(isDisablePerEntryInvalidationEvents());
        target.setEvictionConfig(getEvictionConfig());
        target.setHotRestartConfig(getHotRestartConfig());
        target.setInMemoryFormat(getInMemoryFormat());
        if (resolved) {
            target.setKeyType(getKeyType());
            target.setValueType(getValueType());
        } else {
            target.setKeyClassName(getKeyClassName());
            target.setValueClassName(getValueClassName());
        }

        target.cacheLoaderFactory = cacheLoaderFactory.shallowCopy();
        target.cacheWriterFactory = cacheWriterFactory.shallowCopy();
        target.expiryPolicyFactory = expiryPolicyFactory.shallowCopy();

        target.listenerConfigurations = createConcurrentSet();
        for (DeferredValue<CacheEntryListenerConfiguration<K, V>> lazyEntryListenerConfig : listenerConfigurations) {
            target.listenerConfigurations.add(lazyEntryListenerConfig.shallowCopy());
        }

        target.setManagementEnabled(isManagementEnabled());
        target.setManagerPrefix(getManagerPrefix());
        target.setMergePolicy(getMergePolicy());
        target.setName(getName());
        target.setPartitionLostListenerConfigs(getPartitionLostListenerConfigs());
        target.setQuorumName(getQuorumName());
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
            MutableCacheEntryListenerConfiguration<K, V> listenerConfiguration =
                    new MutableCacheEntryListenerConfiguration<K, V>(
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
                    ClassLoaderUtil.<Factory<? extends CacheLoader<K, V>>>newInstance(
                            null,
                            simpleConfig.getCacheLoaderFactory()
                    )
            );
        }
        if (simpleConfig.getCacheLoader() != null) {
            setCacheLoaderFactory(FactoryBuilder.<CacheLoader<K, V>>factoryOf(simpleConfig.getCacheLoader()));
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
