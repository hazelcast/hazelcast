/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.ValidationUtil.isNotNull;

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
    public static final int MAX_BACKUP_COUNT = 6;

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
    public static final long DEFAULT_CHECK_IF_EVICTABLE_AFTER_MILLIS = 0L;

    /**
     * The number of default Time to Live seconds
     */
    public static final int DEFAULT_TTL_SECONDS = 0;

    /**
     * The number of default time to wait eviction
     */
    public static final int DEFAULT_MAX_IDLE_SECONDS = 0;
    /**
     * Maximum size
     */
    public static final int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;
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

    private long checkIfEvictableAfterMillis = DEFAULT_CHECK_IF_EVICTABLE_AFTER_MILLIS;

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

    private List<EntryListenerConfig> listenerConfigs;

    private List<MapIndexConfig> mapIndexConfigs;

    private boolean statisticsEnabled = true;

    private PartitioningStrategyConfig partitioningStrategyConfig;

    private MapConfigReadOnly readOnly;

    /**
     * Eviction Policy enum
     */
    public enum EvictionPolicy {
        /**
         * Least Recently Used
         */
        LRU,
        /**
         * Least Frequently Used
         */
        LFU,
        /**
         * None
         */
        NONE
    }

    public MapConfig(String name) {
        this.name = name;
    }

    public MapConfig() {
    }

    public MapConfig(MapConfig config) {
        this.name = config.name;
        this.backupCount = config.backupCount;
        this.asyncBackupCount = config.asyncBackupCount;
        this.evictionPercentage = config.evictionPercentage;
        this.checkIfEvictableAfterMillis = config.checkIfEvictableAfterMillis;
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
        this.listenerConfigs = new ArrayList<EntryListenerConfig>(config.getEntryListenerConfigs());
        this.mapIndexConfigs = new ArrayList<MapIndexConfig>(config.getMapIndexConfigs());
        this.partitioningStrategyConfig = config.partitioningStrategyConfig != null
                ? new PartitioningStrategyConfig(config.getPartitioningStrategyConfig()) : null;
    }

    public MapConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MapConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public MapConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * @return data type that will be used for storing records.
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Data type that will be used for storing records.
     * Possible values:
     * BINARY (default): keys and values will be stored as binary data
     * OBJECT : values will be stored in their object forms
     * OFFHEAP : values will be stored in non-heap region of JVM
     *
     * @param inMemoryFormat the record type to set
     * @throws IllegalArgumentException if inMemoryFormat is null.
     */
    public MapConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = isNotNull(inMemoryFormat, "inMemoryFormat");
        return this;
    }

    /**
     * @return the backupCount
     * @see #getAsyncBackupCount()
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Number of synchronous backups. If 1 is set as the backup-count for example,
     * then all entries of the map will be copied to another JVM for
     * fail-safety. 0 means no sync backup.
     *
     * @param backupCount the backupCount to set
     * @see #setAsyncBackupCount(int)
     */
    public MapConfig setBackupCount(final int backupCount) {
        if (backupCount < MIN_BACKUP_COUNT) {
            throw new IllegalArgumentException("map backup count must be equal to or bigger than "
                    + MIN_BACKUP_COUNT);
        }
        if ((backupCount + this.asyncBackupCount) > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("total (sync + async) map backup count must be less than "
                    + MAX_BACKUP_COUNT);
        }
        this.backupCount = backupCount;
        return this;
    }

    /**
     * @return the asyncBackupCount
     * @see #setBackupCount(int)
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Number of asynchronous backups.
     * 0 means no backup.
     *
     * @param asyncBackupCount the asyncBackupCount to set
     * @see #setBackupCount(int)
     */
    public MapConfig setAsyncBackupCount(final int asyncBackupCount) {
        if (asyncBackupCount < MIN_BACKUP_COUNT) {
            throw new IllegalArgumentException("map async backup count must be equal to or bigger than "
                    + MIN_BACKUP_COUNT);
        }
        if ((this.backupCount + asyncBackupCount) > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("total (sync + async) map backup count must be less than "
                    + MAX_BACKUP_COUNT);
        }
        this.asyncBackupCount = asyncBackupCount;
        return this;
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * @return the evictionPercentage
     */
    public int getEvictionPercentage() {
        return evictionPercentage;
    }

    /**
     * When max. size is reached, specified percentage of the map will be evicted.
     * Any integer between 0 and 100 is allowed.
     * If 25 is set for example, 25% of the entries will get evicted.
     *
     * @param evictionPercentage the evictionPercentage to set
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
     * Returns minimum milliseconds which should pass before asking if a partition of this map is evictable or not.
     * <p/>
     * Default value is {@value #DEFAULT_CHECK_IF_EVICTABLE_AFTER_MILLIS} milliseconds.
     *
     * @return Number of milliseconds should pass before asking next eviction.
     * @since 3.3
     */
    public long getCheckIfEvictableAfterMillis() {
        return checkIfEvictableAfterMillis;
    }

    /**
     * Sets the minimum time in millis which should pass before asking if a partition of this map is evictable or not.
     * <p/>
     * Default value is {@value #DEFAULT_CHECK_IF_EVICTABLE_AFTER_MILLIS} milliseconds.
     *
     * @param checkIfEvictableAfterMillis time in millis.
     * @since 3.3
     */
    public void setCheckIfEvictableAfterMillis(long checkIfEvictableAfterMillis) {
        if (checkIfEvictableAfterMillis < 0) {
            throw new IllegalArgumentException("Parameter checkIfEvictableAfterMillis must be greater or equal than 0");
        }
        this.checkIfEvictableAfterMillis = checkIfEvictableAfterMillis;
    }

    /**
     * @return the timeToLiveSeconds
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * Maximum number of seconds for each entry to stay in the map. Entries that are
     * older than timeToLiveSeconds will get automatically evicted from the map.
     * Updates on the entry don't change the eviction time.
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
     * @return the maxIdleSeconds
     */
    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    /**
     * Maximum number of seconds for each entry to stay idle in the map. Entries that are
     * idle(not touched) for more than maxIdleSeconds will get
     * automatically evicted from the map. Entry is touched if get, put or
     * containsKey is called.
     * Any integer between 0 and Integer.MAX_VALUE.
     * 0 means infinite. Default is 0.
     *
     * @param maxIdleSeconds the maxIdleSeconds to set
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
     * @return the evictionPolicy
     */
    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    /**
     * @param evictionPolicy the evictionPolicy to set
     */
    public MapConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        return this;
    }

    /**
     * Returns the map store configuration
     *
     * @return the mapStoreConfig
     */
    public MapStoreConfig getMapStoreConfig() {
        return mapStoreConfig;
    }

    /**
     * Sets the mapStore configuration
     *
     * @param mapStoreConfig the mapStoreConfig to set
     */
    public MapConfig setMapStoreConfig(MapStoreConfig mapStoreConfig) {
        this.mapStoreConfig = mapStoreConfig;
        return this;
    }

    public NearCacheConfig getNearCacheConfig() {
        return nearCacheConfig;
    }

    public MapConfig setNearCacheConfig(NearCacheConfig nearCacheConfig) {
        this.nearCacheConfig = nearCacheConfig;
        return this;
    }

    public String getMergePolicy() {
        return mergePolicy;
    }

    public MapConfig setMergePolicy(String mergePolicy) {
        this.mergePolicy = mergePolicy;
        return this;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public MapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    public boolean isReadBackupData() {
        return readBackupData;
    }

    public MapConfig setReadBackupData(boolean readBackupData) {
        this.readBackupData = readBackupData;
        return this;
    }

    public WanReplicationRef getWanReplicationRef() {
        return wanReplicationRef;
    }

    public MapConfig setWanReplicationRef(WanReplicationRef wanReplicationRef) {
        this.wanReplicationRef = wanReplicationRef;
        return this;
    }

    public MapConfig addEntryListenerConfig(EntryListenerConfig listenerConfig) {
        getEntryListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<EntryListenerConfig> getEntryListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<EntryListenerConfig>();
        }
        return listenerConfigs;
    }

    public MapConfig setEntryListenerConfigs(List<EntryListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
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

    public PartitioningStrategyConfig getPartitioningStrategyConfig() {
        return partitioningStrategyConfig;
    }

    public MapConfig setPartitioningStrategyConfig(PartitioningStrategyConfig partitioningStrategyConfig) {
        this.partitioningStrategyConfig = partitioningStrategyConfig;
        return this;
    }

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
                && this.checkIfEvictableAfterMillis == other.checkIfEvictableAfterMillis
                && this.maxIdleSeconds == other.maxIdleSeconds
                && (this.maxSizeConfig.getSize() == other.maxSizeConfig.getSize()
                || (Math.min(maxSizeConfig.getSize(), other.maxSizeConfig.getSize()) == 0
                && Math.max(maxSizeConfig.getSize(), other.maxSizeConfig.getSize()) == Integer.MAX_VALUE))
                && this.timeToLiveSeconds == other.timeToLiveSeconds
                && this.readBackupData == other.readBackupData;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.backupCount;
        result = prime * result + this.asyncBackupCount;
        result = prime * result + this.evictionPercentage;
        result = prime * result + (int) (checkIfEvictableAfterMillis ^ (checkIfEvictableAfterMillis >>> 32));
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
                        && this.checkIfEvictableAfterMillis == other.checkIfEvictableAfterMillis
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
        sb.append(", checkIfEvictableAfterMillis=").append(checkIfEvictableAfterMillis);
        sb.append(", maxSizeConfig=").append(maxSizeConfig);
        sb.append(", readBackupData=").append(readBackupData);
        sb.append(", nearCacheConfig=").append(nearCacheConfig);
        sb.append(", mapStoreConfig=").append(mapStoreConfig);
        sb.append(", mergePolicyConfig='").append(mergePolicy).append('\'');
        sb.append(", wanReplicationRef=").append(wanReplicationRef);
        sb.append(", listenerConfigs=").append(listenerConfigs);
        sb.append(", mapIndexConfigs=").append(mapIndexConfigs);
        sb.append('}');
        return sb.toString();
    }
}
