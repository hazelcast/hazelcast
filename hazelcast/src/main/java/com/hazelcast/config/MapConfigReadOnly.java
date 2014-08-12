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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Contains the configuration for an {@link com.hazelcast.core.IMap} (read-only).
 */
public class MapConfigReadOnly extends MapConfig {

    MapConfigReadOnly(MapConfig config) {
        super(config);
    }

    public MaxSizeConfig getMaxSizeConfig() {
        final MaxSizeConfig maxSizeConfig = super.getMaxSizeConfig();
        if (maxSizeConfig == null) {
            return null;
        }
        return maxSizeConfig.getAsReadOnly();
    }

    public WanReplicationRef getWanReplicationRef() {
        final WanReplicationRef wanReplicationRef = super.getWanReplicationRef();
        if (wanReplicationRef == null) {
            return null;
        }
        return wanReplicationRef.getAsReadOnly();
    }

    public List<EntryListenerConfig> getEntryListenerConfigs() {
        final List<EntryListenerConfig> listenerConfigs = super.getEntryListenerConfigs();
        final List<EntryListenerConfig> readOnlyListenerConfigs = new ArrayList<EntryListenerConfig>(listenerConfigs.size());
        for (EntryListenerConfig listenerConfig : listenerConfigs) {
            readOnlyListenerConfigs.add(listenerConfig.getAsReadOnly());
        }
        return Collections.unmodifiableList(readOnlyListenerConfigs);
    }

    public List<MapIndexConfig> getMapIndexConfigs() {
        final List<MapIndexConfig> mapIndexConfigs = super.getMapIndexConfigs();
        final List<MapIndexConfig> readOnlyMapIndexConfigs = new ArrayList<MapIndexConfig>(mapIndexConfigs.size());
        for (MapIndexConfig mapIndexConfig : mapIndexConfigs) {
            readOnlyMapIndexConfigs.add(mapIndexConfig.getAsReadOnly());
        }
        return Collections.unmodifiableList(readOnlyMapIndexConfigs);
    }

    public PartitioningStrategyConfig getPartitioningStrategyConfig() {
        final PartitioningStrategyConfig partitioningStrategyConfig = super.getPartitioningStrategyConfig();
        if (partitioningStrategyConfig == null) {
            return null;
        }
        return partitioningStrategyConfig.getAsReadOnly();
    }

    public MapStoreConfig getMapStoreConfig() {
        final MapStoreConfig mapStoreConfig = super.getMapStoreConfig();
        if (mapStoreConfig == null) {
            return null;
        }
        return mapStoreConfig.getAsReadOnly();
    }

    public NearCacheConfig getNearCacheConfig() {
        final NearCacheConfig nearCacheConfig = super.getNearCacheConfig();
        if (nearCacheConfig == null) {
            return null;
        }
        return nearCacheConfig.getAsReadOnly();
    }

    public MapConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setBackupCount(int backupCount) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setAsyncBackupCount(int asyncBackupCount) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setEvictionPercentage(int evictionPercentage) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    @Override
    public void setMinEvictionCheckMillis(long checkIfEvictableAfterMillis) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setMaxIdleSeconds(int maxIdleSeconds) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setMaxSizeConfig(MaxSizeConfig maxSizeConfig) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setMapStoreConfig(MapStoreConfig mapStoreConfig) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setNearCacheConfig(NearCacheConfig nearCacheConfig) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setMergePolicy(String mergePolicy) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setReadBackupData(boolean readBackupData) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setWanReplicationRef(WanReplicationRef wanReplicationRef) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig addEntryListenerConfig(EntryListenerConfig listenerConfig) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setEntryListenerConfigs(List<EntryListenerConfig> listenerConfigs) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig addMapIndexConfig(MapIndexConfig mapIndexConfig) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setMapIndexConfigs(List<MapIndexConfig> mapIndexConfigs) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setPartitioningStrategyConfig(PartitioningStrategyConfig partitioningStrategyConfig) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

    public MapConfig setOptimizeQueries(boolean optimizeQueries) {
        throw new UnsupportedOperationException("This config is read-only map: " + getName());
    }

}
