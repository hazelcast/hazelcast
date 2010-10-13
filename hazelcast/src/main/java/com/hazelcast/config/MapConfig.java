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

public class MapConfig {

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

    private String name = null;

    private int backupCount = DEFAULT_BACKUP_COUNT;

    private int evictionPercentage = DEFAULT_EVICTION_PERCENTAGE;

    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;

    private int maxIdleSeconds = DEFAULT_TTL_SECONDS;

    private int evictionDelaySeconds = DEFAULT_EVICTION_DELAY_SECONDS;

    private int maxSize = DEFAULT_MAX_SIZE;

    private String evictionPolicy = DEFAULT_EVICTION_POLICY;

    private boolean valueIndexed = false;

    private MapStoreConfig mapStoreConfig = null;

    private NearCacheConfig nearCacheConfig = null;

    private boolean useBackupData = false;

    private String mergePolicy = DEFAULT_MERGE_POLICY;

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
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * @param maxSize the maxSize to set
     */
    public MapConfig setMaxSize(final int maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("map max size must be greater than 0");
        }
        this.maxSize = maxSize;
        return this;
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

    public boolean isUseBackupData() {
        return useBackupData;
    }

    public MapConfig setUseBackupData(boolean useBackupData) {
        this.useBackupData = useBackupData;
        return this;
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
                ", maxSize=" + maxSize +
                ", evictionPolicy='" + evictionPolicy + '\'' +
                ", mapStoreConfig=" + mapStoreConfig +
                ", nearCacheConfig=" + nearCacheConfig +
                ", useBackupData=" + useBackupData +
                '}';
    }
}
