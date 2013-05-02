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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.ByteUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapConfig implements DataSerializable {

    public final static int MIN_BACKUP_COUNT = 0;
    public final static int DEFAULT_BACKUP_COUNT = 1;
    public final static int MAX_BACKUP_COUNT = 6;

    public final static int MIN_EVICTION_PERCENTAGE = 0;
    public final static int DEFAULT_EVICTION_PERCENTAGE = 25;
    public final static int MAX_EVICTION_PERCENTAGE = 100;

    public final static int DEFAULT_EVICTION_DELAY_SECONDS = 3;
    public final static int DEFAULT_TTL_SECONDS = 0;
    public final static int DEFAULT_MAX_IDLE_SECONDS = 0;
    public final static int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;
    public final static EvictionPolicy DEFAULT_EVICTION_POLICY = EvictionPolicy.NONE;
    public final static String DEFAULT_MAP_MERGE_POLICY = PutIfAbsentMapMergePolicy.class.getName();
    public final static InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.BINARY;

    private String name = null;

    private int backupCount = DEFAULT_BACKUP_COUNT;

    private int asyncBackupCount = MIN_BACKUP_COUNT;

    private int evictionPercentage = DEFAULT_EVICTION_PERCENTAGE;

    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;

    private int maxIdleSeconds = DEFAULT_TTL_SECONDS;

    private int evictionDelaySeconds = DEFAULT_EVICTION_DELAY_SECONDS;

    private MaxSizeConfig maxSizeConfig = new MaxSizeConfig();

    private EvictionPolicy evictionPolicy = DEFAULT_EVICTION_POLICY;

    private MapStoreConfig mapStoreConfig = null;

    private NearCacheConfig nearCacheConfig = null;

    private boolean readBackupData = false;

    private String mergePolicy = DEFAULT_MAP_MERGE_POLICY;

    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;

    private WanReplicationRef wanReplicationRef;

    private List<EntryListenerConfig> listenerConfigs;

    private List<MapIndexConfig> mapIndexConfigs;

    private StorageType storageType = null;

    private boolean statisticsEnabled = true;

    public enum InMemoryFormat {
        BINARY, OBJECT, CACHED
    }

    public enum StorageType {
        HEAP, OFFHEAP
    }

    public enum EvictionPolicy {
        LRU, LFU, NONE
    }

    public MapConfig(String name) {
        this.name = name;
    }

    public MapConfig() {
    }

    public MapConfig(MapConfig config) {
        this.name = config.name;
        this.backupCount = config.backupCount;
        this.evictionPercentage = config.evictionPercentage;
        this.timeToLiveSeconds = config.timeToLiveSeconds;
        this.maxIdleSeconds = config.maxIdleSeconds;
        this.evictionDelaySeconds = config.evictionDelaySeconds;
        this.maxSizeConfig = config.maxSizeConfig;
        this.evictionPolicy = config.evictionPolicy;
        this.inMemoryFormat = config.inMemoryFormat;
        this.mapStoreConfig = config.mapStoreConfig;
        this.nearCacheConfig = config.nearCacheConfig;
        this.readBackupData = config.readBackupData;
        this.statisticsEnabled = config.statisticsEnabled;
        this.mergePolicy = config.mergePolicy;
        this.wanReplicationRef = config.wanReplicationRef;
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
     * CACHED: object form of values will be cached
     *
     * @param inMemoryFormat the record type to set
     */
    public MapConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = inMemoryFormat;
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
     * @see #setBackupCounts(int, int)
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
     * @see #setBackupCounts(int, int)
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

    /**
     * Number of sync and async backups.
     * 0 means no backup.
     *
     * @param backupCount      the sync backup count to set
     * @param asyncBackupCount the async backup count to set
     * @see #setBackupCount(int)
     * @see #setAsyncBackupCount(int)
     */
    public MapConfig setBackupCounts(final int backupCount, final int asyncBackupCount) {
        if (backupCount < MIN_BACKUP_COUNT || asyncBackupCount < MIN_BACKUP_COUNT) {
            throw new IllegalArgumentException("map backup count must be equal to or bigger than "
                    + MIN_BACKUP_COUNT);
        }
        if ((backupCount + asyncBackupCount) > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("total (sync + async) map backup count must be less than "
                    + MAX_BACKUP_COUNT);
        }
        this.backupCount = backupCount;
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
     * @return the evictionDelaySeconds
     * @deprecated
     */
    public int getEvictionDelaySeconds() {
        return evictionDelaySeconds;
    }

    /**
     * @param evictionDelaySeconds the evictionPercentage to set
     * @deprecated
     */
    public MapConfig setEvictionDelaySeconds(int evictionDelaySeconds) {
        this.evictionDelaySeconds = evictionDelaySeconds;
        return this;
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

    public StorageType getStorageType() {
        return storageType;
    }

    public MapConfig setStorageType(StorageType storageType) {
        this.storageType = storageType;
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

    public boolean isNearCacheEnabled() {
        return nearCacheConfig != null;
    }

    public boolean isCompatible(MapConfig other) {
        if (this == other) {
            return true;
        }
        return other != null &&
                (this.name != null ? this.name.equals(other.name) : other.name == null) &&
                this.backupCount == other.backupCount &&
                this.asyncBackupCount == other.asyncBackupCount &&
                this.evictionDelaySeconds == other.evictionDelaySeconds &&
                this.evictionPercentage == other.evictionPercentage &&
                this.maxIdleSeconds == other.maxIdleSeconds &&
                (this.maxSizeConfig.getSize() == other.maxSizeConfig.getSize() ||
                        (Math.min(maxSizeConfig.getSize(), other.maxSizeConfig.getSize()) == 0
                                && Math.max(maxSizeConfig.getSize(), other.maxSizeConfig.getSize()) == Integer.MAX_VALUE)) &&
                this.timeToLiveSeconds == other.timeToLiveSeconds &&
                this.readBackupData == other.readBackupData;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.backupCount;
        result = prime * result + this.asyncBackupCount;
        result = prime * result + this.evictionDelaySeconds;
        result = prime * result + this.evictionPercentage;
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
//        result = prime * result + (this.valueIndexed ? 1231 : 1237);
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
                (this.name != null ? this.name.equals(other.name) : other.name == null) &&
                        this.backupCount == other.backupCount &&
                        this.asyncBackupCount == other.asyncBackupCount &&
                        this.evictionDelaySeconds == other.evictionDelaySeconds &&
                        this.evictionPercentage == other.evictionPercentage &&
                        this.maxIdleSeconds == other.maxIdleSeconds &&
                        this.maxSizeConfig.getSize() == other.maxSizeConfig.getSize() &&
                        this.timeToLiveSeconds == other.timeToLiveSeconds &&
                        this.readBackupData == other.readBackupData &&
//                        this.valueIndexed == other.valueIndexed &&
                        (this.mergePolicy != null ? this.mergePolicy.equals(other.mergePolicy) : other.mergePolicy == null) &&
                        (this.inMemoryFormat != null ? this.inMemoryFormat.equals(other.inMemoryFormat) : other.inMemoryFormat == null) &&
                        (this.evictionPolicy != null ? this.evictionPolicy.equals(other.evictionPolicy)
                                : other.evictionPolicy == null) &&
                        (this.mapStoreConfig != null ? this.mapStoreConfig.equals(other.mapStoreConfig)
                                : other.mapStoreConfig == null) &&
                        (this.nearCacheConfig != null ? this.nearCacheConfig.equals(other.nearCacheConfig)
                                : other.nearCacheConfig == null);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        inMemoryFormat = InMemoryFormat.valueOf(in.readUTF());
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        evictionPercentage = in.readInt();
        timeToLiveSeconds = in.readInt();
        maxIdleSeconds = in.readInt();
        evictionDelaySeconds = in.readInt();
        maxSizeConfig.readData(in);
        boolean[] b = ByteUtil.fromByte(in.readByte());
        readBackupData = b[0];
//        valueIndexed = b[1];
        evictionPolicy = MapConfig.EvictionPolicy.valueOf(in.readUTF());
//        mergePolicyConfig = new MapMergePolicyConfig();
        // TODO: MapStoreConfig mapStoreConfig
        // TODO: NearCacheConfig nearCacheConfig
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(inMemoryFormat.toString());
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        out.writeInt(evictionPercentage);
        out.writeInt(timeToLiveSeconds);
        out.writeInt(maxIdleSeconds);
        out.writeInt(evictionDelaySeconds);
        maxSizeConfig.writeData(out);
        out.writeByte(ByteUtil.toByte(readBackupData));
        out.writeUTF(evictionPolicy.name());
//        out.writeUTF(mergePolicyConfig);
        // TODO: MapStoreConfig mapStoreConfig
        // TODO: NearCacheConfig nearCacheConfig
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
        sb.append(", evictionDelaySeconds=").append(evictionDelaySeconds);
        sb.append(", maxSizeConfig=").append(maxSizeConfig);
        sb.append(", readBackupData=").append(readBackupData);
        sb.append(", nearCacheConfig=").append(nearCacheConfig);
        sb.append(", mapStoreConfig=").append(mapStoreConfig);
        sb.append(", mergePolicyConfig='").append(mergePolicy).append('\'');
        sb.append(", wanReplicationRef=").append(wanReplicationRef);
        sb.append(", listenerConfigs=").append(listenerConfigs);
        sb.append(", mapIndexConfigs=").append(mapIndexConfigs);
        sb.append(", storageType=").append(storageType);
        sb.append('}');
        return sb.toString();
    }
}
