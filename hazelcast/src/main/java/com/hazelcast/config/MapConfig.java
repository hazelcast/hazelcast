/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

import com.hazelcast.merge.LatestUpdateMergePolicy;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.util.ByteUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapConfig implements DataSerializable {

    public final static int MIN_BACKUP_COUNT = 0;
    public final static int DEFAULT_BACKUP_COUNT = 1;
    public final static int MAX_BACKUP_COUNT = 3;

    public final static int MIN_EVICTION_PERCENTAGE = 0;
    public final static int DEFAULT_EVICTION_PERCENTAGE = 25;
    public final static int MAX_EVICTION_PERCENTAGE = 100;

    public final static int DEFAULT_EVICTION_DELAY_SECONDS = 3;
    public final static int DEFAULT_TTL_SECONDS = 0;
    public final static int DEFAULT_MAX_IDLE_SECONDS = 0;
    public final static int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;
    public final static String DEFAULT_EVICTION_POLICY = "NONE";
    public final static String DEFAULT_MERGE_POLICY = LatestUpdateMergePolicy.NAME;
    public final static boolean DEFAULT_CACHE_VALUE = true;

    private String name = null;

    private int backupCount = DEFAULT_BACKUP_COUNT;

    private int evictionPercentage = DEFAULT_EVICTION_PERCENTAGE;

    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;

    private int maxIdleSeconds = DEFAULT_TTL_SECONDS;

    private int evictionDelaySeconds = DEFAULT_EVICTION_DELAY_SECONDS;

    private MaxSizeConfig maxSizeConfig = new MaxSizeConfig();

    private String evictionPolicy = DEFAULT_EVICTION_POLICY;

    private boolean valueIndexed = false;

    private MapStoreConfig mapStoreConfig = null;

    private NearCacheConfig nearCacheConfig = null;

    private boolean readBackupData = false;

    private boolean cacheValue = DEFAULT_CACHE_VALUE;

    private String mergePolicy = DEFAULT_MERGE_POLICY;

    private WanReplicationRef wanReplicationRef;

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
        this.valueIndexed = config.valueIndexed;
        this.mapStoreConfig = config.mapStoreConfig;
        this.nearCacheConfig = config.nearCacheConfig;
        this.readBackupData = config.readBackupData;
        this.cacheValue = config.cacheValue;
        this.mergePolicy = config.mergePolicy;
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
     * Returns if the value of the mapEntry should be indexed for
     * faster containsValue(obj) operations.
     * <p/>
     * Default is false.
     *
     * @return true if value is indexed, false otherwise
     */
    public boolean isValueIndexed() {
        return valueIndexed;
    }

    /**
     * Sets if the value of the map entries should be indexed for
     * faster containsValue(obj) operations.
     * <p/>
     * Default is false.
     *
     * @param valueIndexed
     */
    public void setValueIndexed(boolean valueIndexed) {
        this.valueIndexed = valueIndexed;
    }

    /**
     * Returns if the entry values are cached
     *
     * @return true if cached, false otherwise
     */
    public boolean isCacheValue() {
        return cacheValue;
    }

    /**
     * Sets if entry values should be cached
     *
     * @param cacheValue
     * @return this MapConfig
     */
    public MapConfig setCacheValue(boolean cacheValue) {
        this.cacheValue = cacheValue;
        return this;
    }

    /**
     * @return the backupCount
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Number of backups. If 1 is set as the backup-count for example,
     * then all entries of the map will be copied to another JVM for
     * fail-safety. Valid numbers are 0 (no backup), 1, 2, 3.
     *
     * @param backupCount the backupCount to set
     */
    public MapConfig setBackupCount(final int backupCount) {
        if ((backupCount < MIN_BACKUP_COUNT) || (backupCount > MAX_BACKUP_COUNT)) {
            throw new IllegalArgumentException("map backup count must be 0, 1, 2 or 3");
        }
        this.backupCount = backupCount;
        return this;
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
     */
    public int getEvictionDelaySeconds() {
        return evictionDelaySeconds;
    }

    /**
     * @param evictionDelaySeconds the evictionPercentage to set
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

    /**
     * @return the maxSize
     * @deprecated use MaxSizeConfig.getSize
     */
    public int getMaxSize() {
        return maxSizeConfig.getSize();
    }

    /**
     * @param maxSize the maxSize to set
     * @deprecated use MaxSizeConfig.setSize
     */
    public MapConfig setMaxSize(final int maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("map max size must be greater than 0");
        }
        this.maxSizeConfig.setSize(maxSize);
        return this;
    }

    public MaxSizeConfig getMaxSizeConfig() {
        return maxSizeConfig;
    }

    /**
     * @return the evictionPolicy
     */
    public String getEvictionPolicy() {
        return evictionPolicy;
    }

    /**
     * @param evictionPolicy the evictionPolicy to set
     */
    public MapConfig setEvictionPolicy(String evictionPolicy) {
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

    public MapConfig setMergePolicy(String mergePolicyName) {
        this.mergePolicy = mergePolicyName;
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

    public boolean isCompatible(MapConfig other) {
        if (this == other)
            return true;
        return other != null &&
                (this.name != null ? this.name.equals(other.name) : other.name == null) &&
                this.backupCount == other.backupCount &&
                this.evictionDelaySeconds == other.evictionDelaySeconds &&
                this.evictionPercentage == other.evictionPercentage &&
                this.maxIdleSeconds == other.maxIdleSeconds &&
                (this.maxSizeConfig.getSize() == other.maxSizeConfig.getSize() ||
                        (Math.min(maxSizeConfig.getSize(), other.maxSizeConfig.getSize()) == 0
                                && Math.max(maxSizeConfig.getSize(), other.maxSizeConfig.getSize()) == Integer.MAX_VALUE)) &&
                this.timeToLiveSeconds == other.timeToLiveSeconds &&
                this.readBackupData == other.readBackupData &&
                this.valueIndexed == other.valueIndexed;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.backupCount;
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
        result = prime * result + (this.valueIndexed ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof MapConfig))
            return false;
        MapConfig other = (MapConfig) obj;
        return
                (this.name != null ? this.name.equals(other.name) : other.name == null) &&
                        this.backupCount == other.backupCount &&
                        this.evictionDelaySeconds == other.evictionDelaySeconds &&
                        this.evictionPercentage == other.evictionPercentage &&
                        this.maxIdleSeconds == other.maxIdleSeconds &&
                        this.maxSizeConfig.getSize() == other.maxSizeConfig.getSize() &&
                        this.timeToLiveSeconds == other.timeToLiveSeconds &&
                        this.readBackupData == other.readBackupData &&
                        this.valueIndexed == other.valueIndexed &&
                        (this.mergePolicy != null ? this.mergePolicy.equals(other.mergePolicy) : other.mergePolicy == null) &&
                        (this.evictionPolicy != null ? this.evictionPolicy.equals(other.evictionPolicy) : other.evictionPolicy == null) &&
                        (this.mapStoreConfig != null ? this.mapStoreConfig.equals(other.mapStoreConfig) : other.mapStoreConfig == null) &&
                        (this.nearCacheConfig != null ? this.nearCacheConfig.equals(other.nearCacheConfig) : other.nearCacheConfig == null);
    }

    @Override
    public String toString() {
        return "MapConfig{" +
                "name='" + name + '\'' +
                ", backupCount=" + backupCount +
                ", mergePolicy=" + mergePolicy +
                ", evictionPercentage=" + evictionPercentage +
                ", timeToLiveSeconds=" + timeToLiveSeconds +
                ", maxIdleSeconds=" + maxIdleSeconds +
                ", evictionDelaySeconds=" + evictionDelaySeconds +
                ", maxSizeConfig=" + maxSizeConfig +
                ", evictionPolicy='" + evictionPolicy + '\'' +
                ", mapStoreConfig=" + mapStoreConfig +
                ", nearCacheConfig=" + nearCacheConfig +
                ", readBackupData=" + readBackupData +
                '}';
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
        backupCount = in.readInt();
        evictionPercentage = in.readInt();
        timeToLiveSeconds = in.readInt();
        maxIdleSeconds = in.readInt();
        evictionDelaySeconds = in.readInt();
        maxSizeConfig.readData(in);
        boolean[] b = ByteUtil.fromByte(in.readByte());
        valueIndexed = b[0];
        readBackupData = b[1];
        cacheValue = b[2];
        evictionPolicy = in.readUTF();
        mergePolicy = in.readUTF();
        // TODO: MapStoreConfig mapStoreConfig
        // TODO: NearCacheConfig nearCacheConfig
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(backupCount);
        out.writeInt(evictionPercentage);
        out.writeInt(timeToLiveSeconds);
        out.writeInt(maxIdleSeconds);
        out.writeInt(evictionDelaySeconds);
        maxSizeConfig.writeData(out);
        out.writeByte(ByteUtil.toByte(valueIndexed, readBackupData, cacheValue));
        out.writeUTF(evictionPolicy);
        out.writeUTF(mergePolicy);
        // TODO: MapStoreConfig mapStoreConfig
        // TODO: NearCacheConfig nearCacheConfig
    }
}
