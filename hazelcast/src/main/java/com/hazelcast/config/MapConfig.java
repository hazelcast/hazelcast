/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.config.DataPersistenceAndHotRestartMerger;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.internal.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkNoNullInside;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Contains the configuration for an {@link IMap}.
 */
@SuppressWarnings("MethodCount")
public class MapConfig implements IdentifiedDataSerializable, NamedConfig, Versioned, UserCodeNamespaceAwareConfig<MapConfig> {

    /**
     * The minimum number of backups
     */
    public static final int MIN_BACKUP_COUNT = 0;
    /**
     * The default number of backups
     */
    public static final int DEFAULT_BACKUP_COUNT = 1;
    /**
     * The maximum number of backups
     */
    public static final int MAX_BACKUP_COUNT = IPartition.MAX_BACKUP_COUNT;

    /**
     * The number of Time to Live that represents disabling TTL.
     */
    public static final int DISABLED_TTL_SECONDS = 0;

    /**
     * The number of default Time to Live in seconds.
     */
    public static final int DEFAULT_TTL_SECONDS = DISABLED_TTL_SECONDS;

    /**
     * The number of default time to wait eviction in seconds.
     */
    public static final int DEFAULT_MAX_IDLE_SECONDS = 0;

    /**
     * Default In-Memory format is binary.
     */
    public static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.BINARY;

    /**
     * We want to cache values only when an index is defined.
     */
    public static final CacheDeserializedValues DEFAULT_CACHED_DESERIALIZED_VALUES = CacheDeserializedValues.INDEX_ONLY;

    /**
     * Default metadata policy
     */
    public static final MetadataPolicy DEFAULT_METADATA_POLICY = MetadataPolicy.CREATE_ON_UPDATE;

    /**
     * Default value of whether statistics are enabled or not
     */
    public static final boolean DEFAULT_STATISTICS_ENABLED = true;
    /**
     * Default value of whether per entry statistics are enabled or not
     */
    public static final boolean DEFAULT_ENTRY_STATS_ENABLED = false;
    /**
     * Default max size.
     */
    public static final int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;

    /**
     * Default max size policy
     */
    public static final MaxSizePolicy DEFAULT_MAX_SIZE_POLICY = MaxSizePolicy.PER_NODE;

    /**
     * Default eviction policy
     */
    public static final EvictionPolicy DEFAULT_EVICTION_POLICY = EvictionPolicy.NONE;

    private boolean readBackupData;
    private boolean statisticsEnabled = DEFAULT_STATISTICS_ENABLED;
    private boolean perEntryStatsEnabled = DEFAULT_ENTRY_STATS_ENABLED;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private int asyncBackupCount = MIN_BACKUP_COUNT;
    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;
    private int maxIdleSeconds = DEFAULT_MAX_IDLE_SECONDS;
    private String name;
    private String splitBrainProtectionName;
    private MapStoreConfig mapStoreConfig = new MapStoreConfig().setEnabled(false);
    private NearCacheConfig nearCacheConfig;
    private CacheDeserializedValues cacheDeserializedValues = DEFAULT_CACHED_DESERIALIZED_VALUES;
    private MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;
    private WanReplicationRef wanReplicationRef;
    private List<EntryListenerConfig> entryListenerConfigs;
    private List<MapPartitionLostListenerConfig> partitionLostListenerConfigs;
    // order of index configs is not relevant
    private List<IndexConfig> indexConfigs;
    private List<AttributeConfig> attributeConfigs;
    private List<QueryCacheConfig> queryCacheConfigs;
    private PartitioningStrategyConfig partitioningStrategyConfig;
    private MetadataPolicy metadataPolicy = DEFAULT_METADATA_POLICY;
    private HotRestartConfig hotRestartConfig = new HotRestartConfig();
    private DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
    private MerkleTreeConfig merkleTreeConfig = new MerkleTreeConfig();
    private EventJournalConfig eventJournalConfig = new EventJournalConfig();
    private EvictionConfig evictionConfig = new EvictionConfig()
            .setEvictionPolicy(DEFAULT_EVICTION_POLICY)
            .setMaxSizePolicy(DEFAULT_MAX_SIZE_POLICY)
            .setSize(DEFAULT_MAX_SIZE);
    private TieredStoreConfig tieredStoreConfig = new TieredStoreConfig();
    private List<PartitioningAttributeConfig> partitioningAttributeConfigs;
    private @Nullable String userCodeNamespace = DEFAULT_NAMESPACE;

    public MapConfig() {
    }

    public MapConfig(String name) {
        setName(name);
    }

    @SuppressWarnings("ExecutableStatementCount")
    public MapConfig(MapConfig config) {
        this.name = config.name;
        this.backupCount = config.backupCount;
        this.asyncBackupCount = config.asyncBackupCount;
        this.timeToLiveSeconds = config.timeToLiveSeconds;
        this.maxIdleSeconds = config.maxIdleSeconds;
        this.metadataPolicy = config.metadataPolicy;
        this.evictionConfig = new EvictionConfig(config.evictionConfig);
        this.inMemoryFormat = config.inMemoryFormat;
        this.mapStoreConfig = config.mapStoreConfig != null ? new MapStoreConfig(config.mapStoreConfig) : null;
        this.nearCacheConfig = config.nearCacheConfig != null ? new NearCacheConfig(config.nearCacheConfig) : null;
        this.readBackupData = config.readBackupData;
        this.cacheDeserializedValues = config.cacheDeserializedValues;
        this.statisticsEnabled = config.statisticsEnabled;
        this.perEntryStatsEnabled = config.perEntryStatsEnabled;
        this.mergePolicyConfig = new MergePolicyConfig(config.mergePolicyConfig);
        this.wanReplicationRef = config.wanReplicationRef != null ? new WanReplicationRef(config.wanReplicationRef) : null;
        this.entryListenerConfigs = new ArrayList<>(config.getEntryListenerConfigs());
        this.partitionLostListenerConfigs = new ArrayList<>(config.getPartitionLostListenerConfigs());
        this.indexConfigs = new ArrayList<>(config.getIndexConfigs());
        this.attributeConfigs = new ArrayList<>(config.getAttributeConfigs());
        this.queryCacheConfigs = new ArrayList<>(config.getQueryCacheConfigs());
        this.partitioningStrategyConfig = config.partitioningStrategyConfig != null
                ? new PartitioningStrategyConfig(config.getPartitioningStrategyConfig()) : null;
        this.splitBrainProtectionName = config.splitBrainProtectionName;
        this.hotRestartConfig = new HotRestartConfig(config.hotRestartConfig);
        this.dataPersistenceConfig = new DataPersistenceConfig(config.dataPersistenceConfig);
        this.merkleTreeConfig = new MerkleTreeConfig(config.merkleTreeConfig);
        this.eventJournalConfig = new EventJournalConfig(config.eventJournalConfig);
        this.tieredStoreConfig = new TieredStoreConfig(config.tieredStoreConfig);
        this.partitioningAttributeConfigs = new ArrayList<>(config.getPartitioningAttributeConfigs());
        this.userCodeNamespace = config.userCodeNamespace;
    }

    /**
     * Returns the name of this {@link IMap}
     *
     * @return the name of the {@link IMap}
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the {@link IMap}
     *
     * @param name the name to set for this {@link IMap}
     */
    @Override
    public MapConfig setName(String name) {
        this.name = checkNotNull(name, "Name must not be null");
        return this;
    }

    /**
     * Returns the data type that will be used for storing records.
     *
     * @return data type that will be used for storing records
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Binary type that will be used for storing records.
     * Possible values:
     * <ul>
     * <li>BINARY (default): keys and values will be stored as binary data</li>
     * <li>OBJECT: values will be stored in their object forms</li>
     * <li>NATIVE: values will be stored in non-heap region of JVM (Hazelcast Enterprise only)</li>
     * </ul>
     *
     * @param inMemoryFormat the record type to set for this {@link IMap}
     * @throws IllegalArgumentException if inMemoryFormat is {@code null}
     */
    public MapConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat, "inMemoryFormat");
        return this;
    }

    /**
     * Gets the {@link EvictionConfig} instance of the eviction
     * configuration for this {@link IMap}.
     *
     * @return the {@link EvictionConfig}
     * instance of the eviction configuration
     */
    public EvictionConfig getEvictionConfig() {
        return evictionConfig;
    }

    /**
     * Sets the {@link EvictionConfig} instance for eviction
     * configuration for this {@link IMap}.
     *
     * @param evictionConfig the {@link EvictionConfig}
     *                       instance to set for the eviction configuration
     * @return current map config instance
     */
    public MapConfig setEvictionConfig(EvictionConfig evictionConfig) {
        this.evictionConfig = isNotNull(evictionConfig, "evictionConfig");
        return this;
    }

    /**
     * Returns the backupCount for this {@link IMap}
     *
     * @return the backupCount for this {@link IMap}
     * @see #getAsyncBackupCount()
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Number of synchronous backups. For example, if 1 is set as the backup count,
     * then all entries of the map will be copied to another JVM for fail-safety.
     * 0 means no sync backup.
     *
     * @param backupCount the number of synchronous backups to set for this {@link IMap}
     * @return the updated MapConfig
     * @see #setAsyncBackupCount(int)
     */
    public MapConfig setBackupCount(final int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Returns the asynchronous backup count for this {@link IMap}.
     *
     * @return the asynchronous backup count
     * @see #setBackupCount(int)
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups. 0 means no backups.
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set
     * @return the updated MapConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than
     *                                  0, or larger than the maximum number of backup or the sum of the
     *                                  backups and async backups is larger than the maximum number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public MapConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Returns the total number of backups: backupCount plus asyncBackupCount.
     *
     * @return the total number of backups: synchronous + asynchronous
     */
    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * Returns the maximum number of seconds for each entry to stay in the map.
     *
     * @return the maximum number of seconds for each entry to stay in the map
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * The maximum number of seconds for each entry to stay in the map. Entries that are
     * older than timeToLiveSeconds will be automatically evicted from the map.
     * Updates on the entry will change the eviction time.
     * Any integer between 0 and Integer.MAX_VALUE.
     * 0 means infinite. Default is 0.
     *
     * @param timeToLiveSeconds the timeToLiveSeconds to set
     */
    public MapConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        return this;
    }

    /**
     * Returns the maximum number of seconds for each entry to stay idle in the map.
     *
     * @return the maximum number of seconds for each entry to stay idle in the map
     */
    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    /**
     * Maximum number of seconds for each entry to stay idle in the
     * map. Entries that are idle (not touched) for more than {@code
     * maxIdleSeconds} will get automatically evicted from the map. Entry
     * is touched if {@code get()}, {@code getAll()}, {@code put()} or
     * {@code containsKey()} is called. Any integer between {@code 0}
     * and {@code Integer.MAX_VALUE}. {@code 0} means infinite. Default
     * is {@code 0}. The time precision is limited by 1 second. The
     * MaxIdle that less than 1 second can lead to unexpected behaviour.
     *
     * @param maxIdleSeconds the maxIdleSeconds (the maximum number
     *                       of seconds for each entry to stay idle in the map) to set
     */
    public MapConfig setMaxIdleSeconds(int maxIdleSeconds) {
        this.maxIdleSeconds = maxIdleSeconds;
        return this;
    }

    /**
     * Returns the map store configuration
     *
     * @return the mapStoreConfig (map store configuration)
     */
    public MapStoreConfig getMapStoreConfig() {
        return mapStoreConfig;
    }

    /**
     * Sets the map store configuration
     *
     * @param mapStoreConfig the mapStoreConfig (map store configuration) to set
     */
    public MapConfig setMapStoreConfig(MapStoreConfig mapStoreConfig) {
        this.mapStoreConfig = mapStoreConfig;
        return this;
    }

    /**
     * Returns the Near Cache configuration
     *
     * @return the Near Cache configuration
     */
    public NearCacheConfig getNearCacheConfig() {
        return nearCacheConfig;
    }

    /**
     * Sets the Near Cache configuration
     *
     * @param nearCacheConfig the Near Cache configuration
     * @return the updated map configuration
     */
    public MapConfig setNearCacheConfig(NearCacheConfig nearCacheConfig) {
        this.nearCacheConfig = nearCacheConfig;
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
     * <p>
     * Note that you may need to enable per entry stats
     * via {@link MapConfig#setPerEntryStatsEnabled}
     * to see all fields of entry view in your {@link
     * com.hazelcast.spi.merge.SplitBrainMergePolicy} implementation.
     *
     * @return the updated map configuration
     */
    public MapConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = checkNotNull(mergePolicyConfig,
                "mergePolicyConfig cannot be null!");
        return this;
    }

    /**
     * Checks if statistics are enabled for this map.
     *
     * @return {@code true} if statistics are enabled, {@code false} otherwise
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Set to enable/disable map level statistics for this map.
     * <p>
     * This setting is only for map level stats such as last
     * access time to map, total number of hits etc. For
     * entry level stats see {@link #perEntryStatsEnabled}
     *
     * @param statisticsEnabled {@code true} to
     *                          enable map statistics, {@code false} to disable
     * @return the current map config instance
     * @see #setPerEntryStatsEnabled
     */
    public MapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * Checks if entry level statistics are enabled for this map.
     *
     * @return {@code true} if entry level statistics
     * are enabled, {@code false} otherwise
     * @since 4.2
     */
    public boolean isPerEntryStatsEnabled() {
        return perEntryStatsEnabled;
    }

    /**
     * Set to enable/disable per entry statistics.
     * Its default value is {@code false}.
     * <p>
     * When you enable per entry stats, you can retrieve entry
     * level statistics such as hits, creation time, last access
     * time, last update time, last stored time for an entry.
     *
     * @param perEntryStatsEnabled {@code true} to enable
     *                             entry level statistics, {@code false} to disable
     * @return the current map config instance
     * @since 4.2
     */
    public MapConfig setPerEntryStatsEnabled(boolean perEntryStatsEnabled) {
        this.perEntryStatsEnabled = perEntryStatsEnabled;
        return this;
    }

    /**
     * Checks if read-backup-data (reading local backup entries) is enabled for this map.
     *
     * @return {@code true} if read-backup-data is enabled, {@code false} otherwise
     */
    public boolean isReadBackupData() {
        return readBackupData;
    }

    /**
     * Sets read-backup-data (reading local backup entries) for this map.
     *
     * @param readBackupData {@code true} to enable read-backup-data, {@code false} to disable
     * @return the current map config instance
     */
    public MapConfig setReadBackupData(boolean readBackupData) {
        this.readBackupData = readBackupData;
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
     * @return the current map config instance
     */
    public MapConfig setWanReplicationRef(WanReplicationRef wanReplicationRef) {
        this.wanReplicationRef = wanReplicationRef;
        return this;
    }

    public MapConfig addEntryListenerConfig(EntryListenerConfig listenerConfig) {
        getEntryListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<EntryListenerConfig> getEntryListenerConfigs() {
        if (entryListenerConfigs == null) {
            entryListenerConfigs = new ArrayList<>();
        }
        return entryListenerConfigs;
    }

    public MapConfig setEntryListenerConfigs(List<EntryListenerConfig> listenerConfigs) {
        this.entryListenerConfigs = listenerConfigs;
        return this;
    }

    public MapConfig addMapPartitionLostListenerConfig(MapPartitionLostListenerConfig listenerConfig) {
        getPartitionLostListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<MapPartitionLostListenerConfig> getPartitionLostListenerConfigs() {
        if (partitionLostListenerConfigs == null) {
            partitionLostListenerConfigs = new ArrayList<>();
        }

        return partitionLostListenerConfigs;
    }

    public MapConfig setPartitionLostListenerConfigs(List<MapPartitionLostListenerConfig> listenerConfigs) {
        this.partitionLostListenerConfigs = listenerConfigs;
        return this;
    }

    public MapConfig addIndexConfig(IndexConfig indexConfig) {
        getIndexConfigs().add(indexConfig);
        return this;
    }

    public List<IndexConfig> getIndexConfigs() {
        if (indexConfigs == null) {
            indexConfigs = new ArrayList<>();
        }
        return indexConfigs;
    }

    public MapConfig setIndexConfigs(List<IndexConfig> indexConfigs) {
        this.indexConfigs = indexConfigs;
        return this;
    }

    public MapConfig addAttributeConfig(AttributeConfig attributeConfig) {
        getAttributeConfigs().add(attributeConfig);
        return this;
    }

    public List<AttributeConfig> getAttributeConfigs() {
        if (attributeConfigs == null) {
            attributeConfigs = new ArrayList<>();
        }
        return attributeConfigs;
    }

    public MapConfig setAttributeConfigs(List<AttributeConfig> attributeConfigs) {
        this.attributeConfigs = attributeConfigs;
        return this;
    }

    /**
     * Returns {@link MetadataPolicy} for this map.
     *
     * @return {@link MetadataPolicy} for this map
     */
    public MetadataPolicy getMetadataPolicy() {
        return metadataPolicy;
    }

    /**
     * Sets the metadata policy. See {@link MetadataPolicy} for more
     * information.
     *
     * @param metadataPolicy the metadata policy
     */
    public MapConfig setMetadataPolicy(MetadataPolicy metadataPolicy) {
        this.metadataPolicy = metadataPolicy;
        return this;
    }

    /**
     * Adds a new {@link QueryCacheConfig} to this {@code MapConfig}.
     *
     * @param queryCacheConfig the config to be added
     * @return this {@code MapConfig} instance
     * @throws java.lang.IllegalArgumentException if there is already a {@code QueryCache}
     *                                            with the same {@code QueryCacheConfig#name}
     */
    public MapConfig addQueryCacheConfig(QueryCacheConfig queryCacheConfig) {
        String queryCacheName = queryCacheConfig.getName();
        List<QueryCacheConfig> queryCacheConfigs = getQueryCacheConfigs();
        for (QueryCacheConfig cacheConfig : queryCacheConfigs) {
            checkFalse(cacheConfig.getName().equals(queryCacheName),
                    "A query cache already exists with name = [" + queryCacheName + ']');
        }
        queryCacheConfigs.add(queryCacheConfig);
        return this;
    }

    /**
     * Returns all {@link QueryCacheConfig} instances defined on this {@code MapConfig}.
     *
     * @return all {@link QueryCacheConfig} instances defined on this {@code MapConfig}
     */
    public List<QueryCacheConfig> getQueryCacheConfigs() {
        if (queryCacheConfigs == null) {
            queryCacheConfigs = new ArrayList<>();
        }
        return queryCacheConfigs;
    }

    /**
     * Sets {@link QueryCacheConfig} instances to this {@code MapConfig}.
     *
     * @return this configuration
     */
    public MapConfig setQueryCacheConfigs(List<QueryCacheConfig> queryCacheConfigs) {
        this.queryCacheConfigs = queryCacheConfigs;
        return this;
    }

    public PartitioningStrategyConfig getPartitioningStrategyConfig() {
        return partitioningStrategyConfig;
    }

    public MapConfig setPartitioningStrategyConfig(PartitioningStrategyConfig partitioningStrategyConfig) {
        this.partitioningStrategyConfig = partitioningStrategyConfig;
        return this;
    }

    /**
     * Checks if Near Cache is enabled.
     *
     * @return {@code true} if Near Cache is enabled, {@code false} otherwise
     */
    public boolean isNearCacheEnabled() {
        return nearCacheConfig != null;
    }

    /**
     * Configure de-serialized value caching.
     * Default: {@link CacheDeserializedValues#INDEX_ONLY}
     *
     * @return this {@code MapConfig} instance
     * @see CacheDeserializedValues
     * @since 3.6
     */
    public MapConfig setCacheDeserializedValues(CacheDeserializedValues cacheDeserializedValues) {
        this.cacheDeserializedValues = cacheDeserializedValues;
        return this;
    }

    /**
     * Gets the {@code HotRestartConfig} for this {@code MapConfig}
     *
     * @return hot restart config
     */
    public @Nonnull
    HotRestartConfig getHotRestartConfig() {
        return hotRestartConfig;
    }

    /**
     * Gets the {@code DataPersistenceConfig} for this {@code MapConfig}
     *
     * @return dataPersistenceConfig config
     */
    public @Nonnull
    DataPersistenceConfig getDataPersistenceConfig() {
        return dataPersistenceConfig;
    }

    /**
     * Sets the {@code HotRestartConfig} for this {@code MapConfig}
     *
     * @param hotRestartConfig hot restart config
     * @return this {@code MapConfig} instance
     *
     * @deprecated use {@link MapConfig#setDataPersistenceConfig(DataPersistenceConfig)}
     */
    @Deprecated(since = "5.0")
    public MapConfig setHotRestartConfig(@Nonnull HotRestartConfig hotRestartConfig) {
        this.hotRestartConfig = checkNotNull(hotRestartConfig, "HotRestartConfig cannot be null");

        DataPersistenceAndHotRestartMerger.merge(hotRestartConfig, dataPersistenceConfig);
        return this;
    }

    /**
     * Sets the {@code DataPersistenceConfig} for this {@code MapConfig}
     *
     * @param dataPersistenceConfig dataPersistenceConfig config
     * @return this {@code MapConfig} instance
     */
    public MapConfig setDataPersistenceConfig(@Nonnull DataPersistenceConfig dataPersistenceConfig) {
        this.dataPersistenceConfig = checkNotNull(dataPersistenceConfig, "DataPersistenceConfig cannot be null");

        DataPersistenceAndHotRestartMerger.merge(hotRestartConfig, dataPersistenceConfig);
        return this;
    }

    /**
     * Gets the {@code MerkleTreeConfig} for this {@code MapConfig}
     *
     * @return merkle tree config
     */
    public @Nonnull
    MerkleTreeConfig getMerkleTreeConfig() {
        return merkleTreeConfig;
    }

    /**
     * Sets the {@code MerkleTreeConfig} for this {@code MapConfig}
     *
     * @param merkleTreeConfig merkle tree config
     * @return this {@code MapConfig} instance
     */
    public MapConfig setMerkleTreeConfig(@Nonnull MerkleTreeConfig merkleTreeConfig) {
        this.merkleTreeConfig = checkNotNull(merkleTreeConfig, "MerkleTreeConfig cannot be null");
        return this;
    }

    /**
     * Gets the {@code EventJournalConfig} for this {@code MapConfig}
     *
     * @return event journal config
     */
    public @Nonnull
    EventJournalConfig getEventJournalConfig() {
        return eventJournalConfig;
    }

    /**
     * Sets the {@code EventJournalConfig} for this {@code MapConfig}
     *
     * @param eventJournalConfig event journal config
     * @return this {@code MapConfig} instance
     */
    public MapConfig setEventJournalConfig(@Nonnull EventJournalConfig eventJournalConfig) {
        this.eventJournalConfig = checkNotNull(eventJournalConfig, "eventJournalConfig cannot be null!");
        return this;
    }

    /**
     * Gets the {@code TieredStoreConfig} for this {@code MapConfig}
     *
     * @return tiered-store config
     */
    public TieredStoreConfig getTieredStoreConfig() {
        return tieredStoreConfig;
    }

    /**
     * Sets the {@code TieredStoreConfig} for this {@code MapConfig}
     *
     * @param tieredStoreConfig tiered-store config
     * @return this {@code MapConfig} instance
     */
    public MapConfig setTieredStoreConfig(TieredStoreConfig tieredStoreConfig) {
        this.tieredStoreConfig = checkNotNull(tieredStoreConfig, "tieredStoreConfig cannot be null");
        return this;
    }

    /**
     * Get current value cache settings
     *
     * @return current value cache settings
     * @since 3.6
     */
    public CacheDeserializedValues getCacheDeserializedValues() {
        return cacheDeserializedValues;
    }

    public String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    public MapConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
        return this;
    }

    /**
     * Get Partition Attribute configs used for creation of
     * {@link com.hazelcast.partition.strategy.AttributePartitioningStrategy}
     *
     * @return list of partitioning attribute configs
     */
    public List<PartitioningAttributeConfig> getPartitioningAttributeConfigs() {
        if (partitioningAttributeConfigs == null) {
            partitioningAttributeConfigs = new ArrayList<>();
        }

        return partitioningAttributeConfigs;
    }

    public MapConfig setPartitioningAttributeConfigs(final List<PartitioningAttributeConfig> partitioningAttributeConfigs) {
        checkNoNullInside(partitioningAttributeConfigs,
                "PartitioningAttributeConfig elements can not be null");
        this.partitioningAttributeConfigs = partitioningAttributeConfigs;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public String getUserCodeNamespace() {
        return userCodeNamespace;
    }

    /**
     * Associates the provided Namespace Name with this structure for {@link ClassLoader} awareness.
     * <p>
     * The behaviour of setting this to {@code null} is outlined in the documentation for
     * {@link UserCodeNamespaceAwareConfig#DEFAULT_NAMESPACE}.
     *
     * @param userCodeNamespace The ID of the Namespace to associate with this structure.
     * @return the updated {@link MapConfig} instance
     * @since 5.4
     */
    @Override
    public MapConfig setUserCodeNamespace(@Nullable String userCodeNamespace) {
        this.userCodeNamespace = userCodeNamespace;
        return this;
    }

    @Override
    @SuppressWarnings({"checkstyle:methodlength", "CyclomaticComplexity", "NPathComplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MapConfig that)) {
            return false;
        }

        if (backupCount != that.backupCount) {
            return false;
        }
        if (asyncBackupCount != that.asyncBackupCount) {
            return false;
        }
        if (timeToLiveSeconds != that.timeToLiveSeconds) {
            return false;
        }
        if (maxIdleSeconds != that.maxIdleSeconds) {
            return false;
        }
        if (readBackupData != that.readBackupData) {
            return false;
        }
        if (statisticsEnabled != that.statisticsEnabled) {
            return false;
        }
        if (perEntryStatsEnabled != that.perEntryStatsEnabled) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (!Objects.equals(evictionConfig, that.evictionConfig)) {
            return false;
        }
        if (!Objects.equals(mapStoreConfig, that.mapStoreConfig)) {
            return false;
        }
        if (!Objects.equals(nearCacheConfig, that.nearCacheConfig)) {
            return false;
        }
        if (cacheDeserializedValues != that.cacheDeserializedValues) {
            return false;
        }
        if (!Objects.equals(mergePolicyConfig, that.mergePolicyConfig)) {
            return false;
        }
        if (inMemoryFormat != that.inMemoryFormat) {
            return false;
        }
        if (metadataPolicy != that.metadataPolicy) {
            return false;
        }
        if (!Objects.equals(wanReplicationRef, that.wanReplicationRef)) {
            return false;
        }
        if (!getEntryListenerConfigs().equals(that.getEntryListenerConfigs())) {
            return false;
        }
        if (!getPartitionLostListenerConfigs().equals(that.getPartitionLostListenerConfigs())) {
            return false;
        }
        if (!Set.copyOf(getIndexConfigs()).equals(Set.copyOf(that.getIndexConfigs()))) {
            return false;
        }
        if (!getAttributeConfigs().equals(that.getAttributeConfigs())) {
            return false;
        }
        if (!getQueryCacheConfigs().equals(that.getQueryCacheConfigs())) {
            return false;
        }
        if (!Objects.equals(partitioningStrategyConfig, that.partitioningStrategyConfig)) {
            return false;
        }
        if (!Objects.equals(splitBrainProtectionName, that.splitBrainProtectionName)) {
            return false;
        }
        if (!merkleTreeConfig.equals(that.merkleTreeConfig)) {
            return false;
        }
        if (!eventJournalConfig.equals(that.eventJournalConfig)) {
            return false;
        }
        if (!dataPersistenceConfig.equals(that.dataPersistenceConfig)) {
            return false;
        }
        if (!tieredStoreConfig.equals(that.tieredStoreConfig)) {
            return false;
        }
        if (!getPartitioningAttributeConfigs().equals(that.getPartitioningAttributeConfigs())) {
            return false;
        }
        if (!Objects.equals(userCodeNamespace, that.userCodeNamespace)) {
            return false;
        }

        return hotRestartConfig.equals(that.hotRestartConfig);
    }

    @SuppressWarnings("NPathComplexity")
    @Override
    public final int hashCode() {
        int result = (name != null ? name.hashCode() : 0);
        result = 31 * result + backupCount;
        result = 31 * result + asyncBackupCount;
        result = 31 * result + timeToLiveSeconds;
        result = 31 * result + maxIdleSeconds;
        result = 31 * result + evictionConfig.hashCode();
        result = 31 * result + (mapStoreConfig != null ? mapStoreConfig.hashCode() : 0);
        result = 31 * result + (nearCacheConfig != null ? nearCacheConfig.hashCode() : 0);
        result = 31 * result + (readBackupData ? 1 : 0);
        result = 31 * result + cacheDeserializedValues.hashCode();
        result = 31 * result + (mergePolicyConfig != null ? mergePolicyConfig.hashCode() : 0);
        result = 31 * result + inMemoryFormat.hashCode();
        result = 31 * result + metadataPolicy.hashCode();
        result = 31 * result + (wanReplicationRef != null ? wanReplicationRef.hashCode() : 0);
        result = 31 * result + getEntryListenerConfigs().hashCode();
        result = 31 * result + Set.copyOf(getIndexConfigs()).hashCode();
        result = 31 * result + getAttributeConfigs().hashCode();
        result = 31 * result + getQueryCacheConfigs().hashCode();
        result = 31 * result + getPartitionLostListenerConfigs().hashCode();
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        result = 31 * result + (perEntryStatsEnabled ? 1 : 0);
        result = 31 * result + (partitioningStrategyConfig != null ? partitioningStrategyConfig.hashCode() : 0);
        result = 31 * result + (splitBrainProtectionName != null ? splitBrainProtectionName.hashCode() : 0);
        result = 31 * result + merkleTreeConfig.hashCode();
        result = 31 * result + eventJournalConfig.hashCode();
        result = 31 * result + hotRestartConfig.hashCode();
        result = 31 * result + dataPersistenceConfig.hashCode();
        result = 31 * result + tieredStoreConfig.hashCode();
        result = 31 * result + getPartitioningAttributeConfigs().hashCode();
        result = 31 * result + (userCodeNamespace != null ? userCodeNamespace.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MapConfig{"
                + "name='" + name + '\''
                + ", inMemoryFormat='" + inMemoryFormat + '\''
                + ", metadataPolicy=" + metadataPolicy
                + ", backupCount=" + backupCount
                + ", asyncBackupCount=" + asyncBackupCount
                + ", timeToLiveSeconds=" + timeToLiveSeconds
                + ", maxIdleSeconds=" + maxIdleSeconds
                + ", readBackupData=" + readBackupData
                + ", evictionConfig=" + evictionConfig
                + ", merkleTree=" + merkleTreeConfig
                + ", eventJournal=" + eventJournalConfig
                + ", hotRestart=" + hotRestartConfig
                + ", dataPersistenceConfig=" + dataPersistenceConfig
                + ", nearCacheConfig=" + nearCacheConfig
                + ", mapStoreConfig=" + mapStoreConfig
                + ", mergePolicyConfig=" + mergePolicyConfig
                + ", wanReplicationRef=" + wanReplicationRef
                + ", entryListenerConfigs=" + entryListenerConfigs
                + ", indexConfigs=" + indexConfigs
                + ", attributeConfigs=" + attributeConfigs
                + ", splitBrainProtectionName=" + splitBrainProtectionName
                + ", queryCacheConfigs=" + queryCacheConfigs
                + ", cacheDeserializedValues=" + cacheDeserializedValues
                + ", statisticsEnabled=" + statisticsEnabled
                + ", entryStatsEnabled=" + perEntryStatsEnabled
                + ", tieredStoreConfig=" + tieredStoreConfig
                + ", partitioningAttributeConfigs=" + partitioningAttributeConfigs
                + ", userCodeNamespace=" + userCodeNamespace
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.MAP_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        out.writeInt(timeToLiveSeconds);
        out.writeInt(maxIdleSeconds);
        out.writeObject(evictionConfig);
        out.writeObject(mapStoreConfig);
        out.writeObject(nearCacheConfig);
        out.writeBoolean(readBackupData);
        out.writeString(cacheDeserializedValues.name());
        out.writeObject(mergePolicyConfig);
        out.writeString(inMemoryFormat.name());
        out.writeObject(wanReplicationRef);
        writeNullableList(entryListenerConfigs, out);
        writeNullableList(partitionLostListenerConfigs, out);
        writeNullableList(indexConfigs, out);
        writeNullableList(attributeConfigs, out);
        writeNullableList(queryCacheConfigs, out);
        out.writeBoolean(statisticsEnabled);
        out.writeObject(partitioningStrategyConfig);
        out.writeString(splitBrainProtectionName);
        out.writeObject(hotRestartConfig);
        out.writeObject(merkleTreeConfig);
        out.writeObject(eventJournalConfig);
        out.writeShort(metadataPolicy.getId());
        out.writeBoolean(perEntryStatsEnabled);
        out.writeObject(dataPersistenceConfig);
        out.writeObject(tieredStoreConfig);
        if (out.getVersion().isGreaterOrEqual(Versions.V5_3)) {
            writeNullableList(partitioningAttributeConfigs, out);
        }

        // RU_COMPAT_5_3
        if (out.getVersion().isGreaterOrEqual(Versions.V5_4)) {
            out.writeString(userCodeNamespace);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        timeToLiveSeconds = in.readInt();
        maxIdleSeconds = in.readInt();
        evictionConfig = in.readObject();
        mapStoreConfig = in.readObject();
        nearCacheConfig = in.readObject();
        readBackupData = in.readBoolean();
        cacheDeserializedValues = CacheDeserializedValues.valueOf(in.readString());
        mergePolicyConfig = in.readObject();
        inMemoryFormat = InMemoryFormat.valueOf(in.readString());
        wanReplicationRef = in.readObject();
        entryListenerConfigs = readNullableList(in);
        partitionLostListenerConfigs = readNullableList(in);
        indexConfigs = readNullableList(in);
        attributeConfigs = readNullableList(in);
        queryCacheConfigs = readNullableList(in);
        statisticsEnabled = in.readBoolean();
        partitioningStrategyConfig = in.readObject();
        splitBrainProtectionName = in.readString();
        setHotRestartConfig(in.readObject());
        merkleTreeConfig = in.readObject();
        eventJournalConfig = in.readObject();
        metadataPolicy = MetadataPolicy.getById(in.readShort());
        perEntryStatsEnabled = in.readBoolean();
        setDataPersistenceConfig(in.readObject());
        setTieredStoreConfig(in.readObject());
        if (in.getVersion().isGreaterOrEqual(Versions.V5_3)) {
            partitioningAttributeConfigs = readNullableList(in);
        }

        // RU_COMPAT_5_3
        if (in.getVersion().isGreaterOrEqual(Versions.V5_4)) {
            userCodeNamespace = in.readString();
        }
    }
}
