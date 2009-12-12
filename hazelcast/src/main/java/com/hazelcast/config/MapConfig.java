/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

public class MapConfig {

    public final static int DEFAULT_BACKUP_COUNT = 1;
    public final static int DEFAULT_EVICTION_PERCENTAGE = 25;
    public final static int DEFAULT_TTL_SECONDS = 0;
    public final static int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;
    public final static String DEFAULT_EVICTION_POLICY = "NONE";

    private String name;

    private int backupCount = DEFAULT_BACKUP_COUNT;

    private int evictionPercentage = DEFAULT_EVICTION_PERCENTAGE;

    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;

    private int maxSize = DEFAULT_MAX_SIZE;

    private String evictionPolicy = DEFAULT_EVICTION_POLICY;

    private MapStoreConfig mapStoreConfig = null;

    private NearCacheConfig nearCacheConfig = null;

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the backupCount
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * @param backupCount the backupCount to set
     */
    public void setBackupCount(int backupCount) {
        this.backupCount = backupCount;
    }

    /**
     * @return the evictionPercentage
     */
    public int getEvictionPercentage() {
        return evictionPercentage;
    }

    /**
     * @param evictionPercentage the evictionPercentage to set
     */
    public void setEvictionPercentage(int evictionPercentage) {
        this.evictionPercentage = evictionPercentage;
    }

    /**
     * @return the timeToLiveSeconds
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * @param timeToLiveSeconds the timeToLiveSeconds to set
     */
    public void setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = timeToLiveSeconds;
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
    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
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
    public void setEvictionPolicy(String evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
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
    public void setMapStoreConfig(MapStoreConfig mapStoreConfig) {
        this.mapStoreConfig = mapStoreConfig;
    }

    public NearCacheConfig getNearCacheConfig() {
        return nearCacheConfig;
    }

    public void setNearCacheConfig(NearCacheConfig nearCacheConfig) {
        this.nearCacheConfig = nearCacheConfig;
    }
}
