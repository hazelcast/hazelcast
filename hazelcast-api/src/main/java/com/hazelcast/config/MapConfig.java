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

import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.partition.InternalPartition;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains the configuration for an {@link com.hazelcast.core.IMap}.
 */
public class MapConfig {

    /**
     * The number of minimum backup counter
     */
    public static final int MIN_BACKUP_COUNT = 0;
    /**
     * The number of default backup counter
     */
    public static final int DEFAULT_BACKUP_COUNT = 1;
    /**
     * The number of maximum backup counter
     */
    public static final int MAX_BACKUP_COUNT = InternalPartition.MAX_BACKUP_COUNT;

    /**
     * The number of minimum eviction percentage
     */
    public static final int MIN_EVICTION_PERCENTAGE = 0;
    /**
     * The number of default eviction percentage
     */
    public static final int DEFAULT_EVICTION_PERCENTAGE = 25;
    /**
     * The number of maximum eviction percentage
     */
    public static final int MAX_EVICTION_PERCENTAGE = 100;

    /**
     * Minimum time in milliseconds which should pass before asking
     * if a partition of this map is evictable or not.
     */
    public static final long DEFAULT_MIN_EVICTION_CHECK_MILLIS = 100L;

    /**
     * The number of default Time to Live in seconds
     */
    public static final int DEFAULT_TTL_SECONDS = 0;

    /**
     * The number of default time to wait eviction in seconds
     */
    public static final int DEFAULT_MAX_IDLE_SECONDS = 0;

    /**
     * Default policy for eviction
     */
    public static final EvictionPolicy DEFAULT_EVICTION_POLICY = EvictionPolicy.NONE;
    /**
     * Default policy for merging
     */
    public static final String DEFAULT_MAP_MERGE_POLICY = PutIfAbsentMapMergePolicy.class.getName();
    /**
     * Default In-Memory format is binary
     */
    public static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.BINARY;

    private String name;

    private int backupCount = DEFAULT_BACKUP_COUNT;

    private int asyncBackupCount = MIN_BACKUP_COUNT;

    private int evictionPercentage = DEFAULT_EVICTION_PERCENTAGE;

    private long minEvictionCheckMillis = DEFAULT_MIN_EVICTION_CHECK_MILLIS;

    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;

    private int maxIdleSeconds = DEFAULT_MAX_IDLE_SECONDS;

    private MaxSizeConfig maxSizeConfig = new MaxSizeConfig();

    private EvictionPolicy evictionPolicy = DEFAULT_EVICTION_POLICY;

    private MapStoreConfig mapStoreConfig;

    private NearCacheConfig nearCacheConfig;

    private boolean readBackupData;

    private boolean optimizeQueries;

    private String mergePolicy = DEFAULT_MAP_MERGE_POLICY;

    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;

    private WanReplicationRef wanReplicationRef;

    private List<EntryListenerConfig> entryListenerConfigs;

    private List<MapPartitionLostListenerConfig> partitionLostListenerConfigs;

    private List<MapIndexConfig> mapIndexConfigs;

    private List<QueryCacheConfig> queryCacheConfigs;

    private boolean statisticsEnabled = true;

    private PartitioningStrategyConfig partitioningStrategyConfig;

    private String quorumName;

    private MapConfigReadOnly readOnly;

    public MapConfig(String name) {
        this.name = name;
    }

    public MapConfig() {
    }

    //CHECKSTYLE:OFF
    public MapConfig(MapConfig config) {
        this.name = config.name;
        this.backupCount = config.backupCount;
        this.asyncBackupCount = config.asyncBackupCount;
        this.evictionPercentage = config.evictionPercentage;
        this.minEvictionCheckMillis = config.minEvictionCheckMillis;
        this.timeToLiveSeconds = config.timeToLiveSeconds;
        this.maxIdleSeconds = config.maxIdleSeconds;
        this.maxSizeConfig = config.maxSizeConfig != null ? new MaxSizeConfig(config.maxSizeConfig) : null;
        this.evictionPolicy = config.evictionPolicy;
        this.inMemoryFormat = config.inMemoryFormat;
        this.mapStoreConfig = config.mapStoreConfig != null ? new MapStoreConfig(config.mapStoreConfig) : null;
        this.nearCacheConfig = config.nearCacheConfig != null ? new NearCacheConfig(config.nearCacheConfig) : null;
        this.readBackupData = config.readBackupData;
        this.optimizeQueries = config.optimizeQueries;
        this.statisticsEnabled = config.statisticsEnabled;
        this.mergePolicy = config.mergePolicy;
        this.wanReplicationRef = config.wanReplicationRef != null ? new WanReplicationRef(config.wanReplicationRef) : null;
        this.entryListenerConfigs = new ArrayList<EntryListenerConfig>(config.getEntryListenerConfigs());
        this.partitionLostListenerConfigs =
                new ArrayList<MapPartitionLostListenerConfig>(config.getPartitionLostListenerConfigs());
        this.mapIndexConfigs = new ArrayList<MapIndexConfig>(config.getMapIndexConfigs());
        this.queryCacheConfigs = new ArrayList<QueryCacheConfig>(config.getQueryCacheConfigs());
        this.partitioningStrategyConfig = config.partitioningStrategyConfig != null
                ? new PartitioningStrategyConfig(config.getPartitioningStrategyConfig()) : null;
        this.quorumName = config.quorumName;
    }
    //CHECKSTYLE:ON

    public MapConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MapConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Returns the name of this {@link com.hazelcast.core.IMap}
     *
     * @return the name of the {@link com.hazelcast.core.IMap}
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the {@link com.hazelcast.core.IMap}
     *
     * @param name the name to set for this {@link com.hazelcast.core.IMap}
     */
    public MapConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Returns the data type that will be used for storing records.
     *
     * @return data type that will be used for storing records.
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Binary type that will be used for storing records.
     * Possible values:
     * BINARY (default): keys and values will be stored as binary data
     * OBJECT : values will be stored in their object forms
     * NATIVE : values will be stored in non-heap region of JVM
     *
     * @param inMemoryFormat the record type to set for this {@link com.hazelcast.core.IMap}
     * @throws IllegalArgumentException if inMemoryFormat is null.
     */
    public MapConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat, "inMemoryFormat");
        return this;
    }

    /**
     * Returns the backupCount for this {@link com.hazelcast.core.IMap}
     *
     * @return the backupCount for this {@link com.hazelcast.core.IMap}
     * @see #getAsyncBackupCount()
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Number of synchronous backups. For example, if 1 is set as the backup count,
     * then all entries of the map will be copied to another JVM for
     * fail-safety. 0 means no sync backup.
     *
     * @param backupCount the number of synchronous backups to set for this {@link com.hazelcast.core.IMap}
     * @see #setAsyncBackupCount(int)
     */
    public MapConfig setBackupCount(final int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Returns the asynchronous backup count for this {@link com.hazelcast.core.IMap}.
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
     * @return the updated CacheConfig
     * @throws new IllegalArgumentException if asyncBackupCount smaller than 0,
     *             or larger than the maximum number of backup
     *             or the sum of the backups and async backups is larger than the maximum number of backups
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
     * Returns the evictionPercentage: specified percentage of the map to be evicted
     *
     * @return the evictionPercentage: specified percentage of the map to be evicted
     */
    public int getEvictionPercentage() {
        return evictionPercentage;
    }

    /**
     * When maximum size is reached, the specified percentage of the map will be evicted.
     * Any integer between 0 and 100 is allowed.
     * For example, if 25 is set, 25% of the entries will be evicted.
     *
     * @param evictionPercentage the evictionPercentage to set: the specified percentage of the map to be evicted
     * @throws IllegalArgumentException if evictionPercentage is not in the 0-100 range.
     */
    public MapConfig setEvictionPercentage(final int evictionPercentage) {
        if (evictionPercentage < MIN_EVICTION_PERCENTAGE) {
            throw new IllegalArgumentException("eviction percentage must be greater or equal than 0");
        }
        if (evictionPercentage > MAX_EVICTION_PERCENTAGE) {
            throw new IllegalArgumentException("eviction percentage must be smaller or equal than 100");
        }
        this.evictionPercentage = evictionPercentage;
        return this;
    }

    /**
     * Returns the minimum milliseconds which should pass before asking if a partition of this map is evictable or not.
     * <p/>
     * Default value is {@value #DEFAULT_MIN_EVICTION_CHECK_MILLIS} milliseconds.
     *
     * @return number of milliseconds that should pass before asking for the next eviction.
     * @since 3.3
     */
    public long getMinEvictionCheckMillis() {
        return minEvictionCheckMillis;
    }

    /**
     * Sets the minimum time in milliseconds which should pass before asking if a partition of this map is evictable or not.
     * <p/>
     * Default value is {@value #DEFAULT_MIN_EVICTION_CHECK_MILLIS} milliseconds.
     *
     * @param minEvictionCheckMillis time in milliseconds that should pass before asking for the next eviction
     * @since 3.3
     */
    public MapConfig setMinEvictionCheckMillis(long minEvictionCheckMillis) {
        if (minEvictionCheckMillis < 0) {
            throw new IllegalArgumentException("Parameter minEvictionCheckMillis can not get a negative value");
        }
        this.minEvictionCheckMillis = minEvictionCheckMillis;
        return this;
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
     * Updates on the entry do not change the eviction time.
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
     * Maximum number of seconds for each entry to stay idle in the map. Entries that are
     * idle(not touched) for more than maxIdleSeconds will get
     * automatically evicted from the map. Entry is touched if get, getAll, put or
     * containsKey is called.
     * Any integer between 0 and Integer.MAX_VALUE.
     * 0 means infinite. Default is 0.
     *
     * @param maxIdleSeconds the maxIdleSeconds (the maximum number of seconds for each entry to stay idle in the map) to set
     */
    public MapConfig setMaxIdleSeconds(int maxIdleSeconds) {
        this.maxIdleSeconds = maxIdleSeconds;
        return this;
    }

    public MaxSizeConfig getMaxSizeConfig() {
        return maxSizeConfig;
    }

    public MapConfig setMaxSizeConfig(MaxSizeConfig maxSizeConfig) {
        this.maxSizeConfig = maxSizeConfig;
        return this;
    }

    /**
     * Returns the evictionPolicy
     *
     * @return the evictionPolicy
     */
    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    /**
     * Sets the evictionPolicy
     *
     * @param evictionPolicy the evictionPolicy to set
     */
    public MapConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
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
     * Returns the near cache configuration
     *
     * @return the near cache configuration
     */
    public NearCacheConfig getNearCacheConfig() {
        return nearCacheConfig;
    }

    /**
     * Sets the near cache configuration
     *
     * @param nearCacheConfig the near cache configuration
     * @return the updated map configuration
     */
    public MapConfig setNearCacheConfig(NearCacheConfig nearCacheConfig) {
        this.nearCacheConfig = nearCacheConfig;
        return this;
    }

    /**
     * Gets the map merge policy {@link com.hazelcast.map.merge.MapMergePolicy}
     *
     * @return the updated map configuration
     */
    public String getMergePolicy() {
        return mergePolicy;
    }

    /**
     * Sets the map merge policy {@link com.hazelcast.map.merge.MapMergePolicy}
     *
     * @param mergePolicy the map merge policy to set
     * @return the updated map configuration
     */
    public MapConfig setMergePolicy(String mergePolicy) {
        this.mergePolicy = mergePolicy;
        return this;
    }

    /**
     * Checks if statistics are enabled for this map.
     *
     * @return True if statistics are enabled, false otherwise.
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Sets statistics to enabled or disabled for this map.
     *
     * @param statisticsEnabled True to enable map statistics, false to disable.
     * @return The current map config instance.
     */
    public MapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * Checks if read-backup-data (reading local backup entires) is enabled for this map.
     *
     * @return True if read-backup-data is enabled, false otherwise.
     */
    public boolean isReadBackupData() {
        return readBackupData;
    }

    /**
     * Sets read-backup-data (reading local backup entires) for this map.
     *
     * @param readBackupData True to enable read-backup-data, false to disable.
     * @return The current map config instance.
     */
    public MapConfig setReadBackupData(boolean readBackupData) {
        this.readBackupData = readBackupData;
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
     * @return The current map config instance.
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
            entryListenerConfigs = new ArrayList<EntryListenerConfig>();
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
            partitionLostListenerConfigs = new ArrayList<MapPartitionLostListenerConfig>();
        }

        return partitionLostListenerConfigs;
    }

    public MapConfig setPartitionLostListenerConfigs(List<MapPartitionLostListenerConfig> listenerConfigs) {
        this.partitionLostListenerConfigs = listenerConfigs;
        return this;
    }


    public MapConfig addMapIndexConfig(MapIndexConfig mapIndexConfig) {
        getMapIndexConfigs().add(mapIndexConfig);
        return this;
    }

    public List<MapIndexConfig> getMapIndexConfigs() {
        if (mapIndexConfigs == null) {
            mapIndexConfigs = new ArrayList<MapIndexConfig>();
        }
        return mapIndexConfigs;
    }

    public MapConfig setMapIndexConfigs(List<MapIndexConfig> mapIndexConfigs) {
        this.mapIndexConfigs = mapIndexConfigs;
        return this;
    }

    /**
     * Adds a new {@code queryCacheConfig} to this {@code MapConfig}.
     *
     * @param queryCacheConfig the config to be added.
     * @return this {@code MapConfig} instance.
     * @throws java.lang.IllegalArgumentException if there is already a {@code QueryCache}
     *                                            with the same {@code QueryCacheConfig#name}.
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
     * Returns all {@code QueryCacheConfig} instances defined on this {@code MapConfig}
     *
     * @return all {@code QueryCacheConfig} instances defined on this {@code MapConfig}
     */
    public List<QueryCacheConfig> getQueryCacheConfigs() {
        if (queryCacheConfigs == null) {
            queryCacheConfigs = new ArrayList<QueryCacheConfig>();
        }
        return queryCacheConfigs;
    }

    /**
     * Sets {@code QueryCacheConfig} instances to this {@code MapConfig}
     *
     * @return this {@code MapConfig} instance.
     */
    public void setQueryCacheConfigs(List<QueryCacheConfig> queryCacheConfigs) {
        this.queryCacheConfigs = queryCacheConfigs;
    }

    public PartitioningStrategyConfig getPartitioningStrategyConfig() {
        return partitioningStrategyConfig;
    }

    public MapConfig setPartitioningStrategyConfig(PartitioningStrategyConfig partitioningStrategyConfig) {
        this.partitioningStrategyConfig = partitioningStrategyConfig;
        return this;
    }

    /**
     * Checks if near cache is enabled
     *
     * @return true if near cache is enabled, false otherwise
     */
    public boolean isNearCacheEnabled() {
        return nearCacheConfig != null;
    }

    public boolean isOptimizeQueries() {
        return optimizeQueries;
    }

    public MapConfig setOptimizeQueries(boolean optimizeQueries) {
        this.optimizeQueries = optimizeQueries;
        return this;
    }

    public boolean isCompatible(MapConfig other) {
        if (this == other) {
            return true;
        }
        return other != null
                && (this.name != null ? this.name.equals(other.name) : other.name == null)
                && this.backupCount == other.backupCount
                && this.asyncBackupCount == other.asyncBackupCount
                && this.evictionPercentage == other.evictionPercentage
                && this.minEvictionCheckMillis == other.minEvictionCheckMillis
                && this.maxIdleSeconds == other.maxIdleSeconds
                && (this.maxSizeConfig.getSize() == other.maxSizeConfig.getSize()
                || (Math.min(maxSizeConfig.getSize(), other.maxSizeConfig.getSize()) == 0
                && Math.max(maxSizeConfig.getSize(), other.maxSizeConfig.getSize()) == Integer.MAX_VALUE))
                && this.timeToLiveSeconds == other.timeToLiveSeconds
                && this.readBackupData == other.readBackupData;
    }

    public String getQuorumName() {
        return quorumName;
    }

    public void setQuorumName(String quorumName) {
        this.quorumName = quorumName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.backupCount;
        result = prime * result + this.asyncBackupCount;
        result = prime * result + this.evictionPercentage;
        result = prime * result + (int) (minEvictionCheckMillis ^ (minEvictionCheckMillis >>> 32));
        result = prime
                * result
                + ((this.evictionPolicy == null) ? 0 : this.evictionPolicy
                .hashCode());
        result = prime
                * result
                + ((this.mapStoreConfig == null) ? 0 : this.mapStoreConfig
                .hashCode());
        result = prime * result + this.maxIdleSeconds;
        result = prime * result + this.maxSizeConfig.getSize();
        result = prime
                * result
                + ((this.mergePolicy == null) ? 0 : this.mergePolicy.hashCode());
        result = prime * result
                + ((this.name == null) ? 0 : this.name.hashCode());
        result = prime
                * result
                + ((this.nearCacheConfig == null) ? 0 : this.nearCacheConfig
                .hashCode());
        result = prime * result + this.timeToLiveSeconds;
        result = prime * result + (this.readBackupData ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MapConfig)) {
            return false;
        }
        MapConfig other = (MapConfig) obj;
        return
                (this.name != null ? this.name.equals(other.name) : other.name == null)
                        && this.backupCount == other.backupCount
                        && this.asyncBackupCount == other.asyncBackupCount
                        && this.evictionPercentage == other.evictionPercentage
                        && this.minEvictionCheckMillis == other.minEvictionCheckMillis
                        && this.maxIdleSeconds == other.maxIdleSeconds
                        && this.maxSizeConfig.getSize() == other.maxSizeConfig.getSize()
                        && this.timeToLiveSeconds == other.timeToLiveSeconds
                        && this.readBackupData == other.readBackupData
                        && (this.mergePolicy != null ? this.mergePolicy.equals(other.mergePolicy) : other.mergePolicy == null)
                        && (this.inMemoryFormat != null ? this.inMemoryFormat.equals(other.inMemoryFormat)
                        : other.inMemoryFormat == null)
                        && (this.evictionPolicy != null ? this.evictionPolicy.equals(other.evictionPolicy)
                        : other.evictionPolicy == null)
                        && (this.mapStoreConfig != null ? this.mapStoreConfig.equals(other.mapStoreConfig)
                        : other.mapStoreConfig == null)
                        && (this.nearCacheConfig != null ? this.nearCacheConfig.equals(other.nearCacheConfig)
                        : other.nearCacheConfig == null);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MapConfig");
        sb.append("{name='").append(name).append('\'');
        sb.append(", inMemoryFormat=").append(inMemoryFormat).append('\'');
        sb.append(", backupCount=").append(backupCount);
        sb.append(", asyncBackupCount=").append(asyncBackupCount);
        sb.append(", timeToLiveSeconds=").append(timeToLiveSeconds);
        sb.append(", maxIdleSeconds=").append(maxIdleSeconds);
        sb.append(", evictionPolicy='").append(evictionPolicy).append('\'');
        sb.append(", evictionPercentage=").append(evictionPercentage);
        sb.append(", minEvictionCheckMillis=").append(minEvictionCheckMillis);
        sb.append(", maxSizeConfig=").append(maxSizeConfig);
        sb.append(", readBackupData=").append(readBackupData);
        sb.append(", nearCacheConfig=").append(nearCacheConfig);
        sb.append(", mapStoreConfig=").append(mapStoreConfig);
        sb.append(", mergePolicyConfig='").append(mergePolicy).append('\'');
        sb.append(", wanReplicationRef=").append(wanReplicationRef);
        sb.append(", entryListenerConfigs=").append(entryListenerConfigs);
        sb.append(", mapIndexConfigs=").append(mapIndexConfigs);
        sb.append(", quorumName=").append(quorumName);
        sb.append(", queryCacheConfigs=").append(queryCacheConfigs);
        sb.append('}');
        return sb.toString();
    }
}
