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

package com.hazelcast.internal.config;

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.TieredStoreConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.internal.util.CollectionUtil;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Read only equivalent of {@link MapConfig}
 *
 * @see MapConfig
 */
@SuppressWarnings({"checkstyle:methodcount",
        "checkstyle:executablestatementcount"})
public class MapConfigReadOnly extends MapConfig {

    private final EvictionConfigReadOnly evictionConfigReadOnly;
    private final WanReplicationRefReadOnly wanReplicationRefReadOnly;
    private final MapStoreConfigReadOnly mapStoreConfigReadOnly;
    private final NearCacheConfigReadOnly nearCacheConfigReadOnly;
    private final HotRestartConfigReadOnly hotRestartConfigReadOnly;
    private final DataPersistenceConfigReadOnly dataPersistenceConfigReadOnly;
    private final EventJournalConfigReadOnly eventJournalConfigReadOnly;
    private final MerkleTreeConfigReadOnly merkleTreeConfigReadOnly;
    private final PartitioningStrategyConfigReadOnly partitioningStrategyConfigReadOnly;
    private final List<IndexConfig> indexConfigReadOnly;
    private final List<MapPartitionLostListenerConfig> partitionLostListenerConfigsReadOnly;
    private final List<QueryCacheConfig> queryCacheConfigsReadOnly;
    private final List<EntryListenerConfig> entryListenerConfigsReadOnly;
    private final TieredStoreConfigReadOnly tieredStoreConfigReadOnly;

    public MapConfigReadOnly(MapConfig config) {
        super(config);

        EvictionConfig evictionConfig = super.getEvictionConfig();
        this.evictionConfigReadOnly = new EvictionConfigReadOnly(evictionConfig);

        WanReplicationRef wanReplicationRef = super.getWanReplicationRef();
        wanReplicationRefReadOnly = wanReplicationRef == null
                ? null : new WanReplicationRefReadOnly(wanReplicationRef);

        MapStoreConfig mapStoreConfig = super.getMapStoreConfig();
        mapStoreConfigReadOnly = mapStoreConfig == null
                ? null : new MapStoreConfigReadOnly(mapStoreConfig);

        NearCacheConfig nearCacheConfig = super.getNearCacheConfig();
        nearCacheConfigReadOnly = nearCacheConfig == null
                ? null : new NearCacheConfigReadOnly(nearCacheConfig);

        HotRestartConfig hotRestartConfig = super.getHotRestartConfig();
        hotRestartConfigReadOnly = new HotRestartConfigReadOnly(hotRestartConfig);

        DataPersistenceConfig dataPersistenceConfig = super.getDataPersistenceConfig();
        dataPersistenceConfigReadOnly = new DataPersistenceConfigReadOnly(dataPersistenceConfig);

        EventJournalConfig eventJournalConfig = super.getEventJournalConfig();
        eventJournalConfigReadOnly = new EventJournalConfigReadOnly(eventJournalConfig);

        MerkleTreeConfig merkleTreeConfig = super.getMerkleTreeConfig();
        merkleTreeConfigReadOnly = new MerkleTreeConfigReadOnly(merkleTreeConfig);

        PartitioningStrategyConfig partitioningStrategyConfig = super.getPartitioningStrategyConfig();
        partitioningStrategyConfigReadOnly = partitioningStrategyConfig == null
                ? null : new PartitioningStrategyConfigReadOnly(partitioningStrategyConfig);

        indexConfigReadOnly = getIndexConfigReadOnly();
        partitionLostListenerConfigsReadOnly = getPartitionLostListenerConfigsReadOnly();
        queryCacheConfigsReadOnly = getQueryCacheConfigsReadOnly();
        entryListenerConfigsReadOnly = getEntryListenerConfigsReadOnly();

        TieredStoreConfig tieredStoreConfig = super.getTieredStoreConfig();
        tieredStoreConfigReadOnly = new TieredStoreConfigReadOnly(tieredStoreConfig);
    }

    private List<EntryListenerConfig> getEntryListenerConfigsReadOnly() {
        List<EntryListenerConfig> entryListenerConfigs = super.getEntryListenerConfigs();
        if (CollectionUtil.isEmpty(entryListenerConfigs)) {
            return Collections.emptyList();
        }

        List<EntryListenerConfig> readOnlyEntryListenerConfigs = new ArrayList<>(entryListenerConfigs.size());
        for (EntryListenerConfig entryListenerConfig : entryListenerConfigs) {
            readOnlyEntryListenerConfigs.add(new EntryListenerConfigReadOnly(entryListenerConfig));
        }
        return Collections.unmodifiableList(readOnlyEntryListenerConfigs);
    }

    private List<QueryCacheConfig> getQueryCacheConfigsReadOnly() {
        List<QueryCacheConfig> queryCacheConfigs = super.getQueryCacheConfigs();
        if (CollectionUtil.isEmpty(queryCacheConfigs)) {
            return Collections.emptyList();
        }

        List<QueryCacheConfig> readOnlyOnes = new ArrayList<>(queryCacheConfigs.size());
        for (QueryCacheConfig queryCacheConfig : queryCacheConfigs) {
            readOnlyOnes.add(new QueryCacheConfigReadOnly(queryCacheConfig));
        }
        return Collections.unmodifiableList(readOnlyOnes);
    }

    private List<MapPartitionLostListenerConfig> getPartitionLostListenerConfigsReadOnly() {
        List<MapPartitionLostListenerConfig> listenerConfigs = super.getPartitionLostListenerConfigs();
        if (CollectionUtil.isEmpty(listenerConfigs)) {
            return Collections.emptyList();
        }

        List<MapPartitionLostListenerConfig> readOnlyListenerConfigs = new ArrayList<>(listenerConfigs.size());
        for (MapPartitionLostListenerConfig listenerConfig : listenerConfigs) {
            readOnlyListenerConfigs.add(new MapPartitionLostListenerConfigReadOnly(listenerConfig));
        }
        return Collections.unmodifiableList(readOnlyListenerConfigs);
    }

    private List<IndexConfig> getIndexConfigReadOnly() {
        List<IndexConfig> indexConfigs = super.getIndexConfigs();
        if (CollectionUtil.isEmpty(indexConfigs)) {
            return Collections.emptyList();
        }

        List<IndexConfig> readOnlyIndexConfigs = new ArrayList<>(indexConfigs.size());
        for (IndexConfig indexConfig : indexConfigs) {
            readOnlyIndexConfigs.add(new IndexConfigReadOnly(indexConfig));
        }
        return Collections.unmodifiableList(readOnlyIndexConfigs);
    }

    @Override
    public EvictionConfig getEvictionConfig() {
        return evictionConfigReadOnly;
    }

    @Override
    public WanReplicationRef getWanReplicationRef() {
        return wanReplicationRefReadOnly;
    }

    @Override
    public List<EntryListenerConfig> getEntryListenerConfigs() {
        return entryListenerConfigsReadOnly;
    }

    @Override
    public List<MapPartitionLostListenerConfig> getPartitionLostListenerConfigs() {
        return partitionLostListenerConfigsReadOnly;
    }

    @Override
    public List<IndexConfig> getIndexConfigs() {
        return indexConfigReadOnly;
    }

    @Override
    public PartitioningStrategyConfig getPartitioningStrategyConfig() {
        return partitioningStrategyConfigReadOnly;
    }

    @Override
    public MapStoreConfig getMapStoreConfig() {
        return mapStoreConfigReadOnly;
    }

    @Override
    public NearCacheConfig getNearCacheConfig() {
        return nearCacheConfigReadOnly;
    }

    @Override
    public List<QueryCacheConfig> getQueryCacheConfigs() {
        return queryCacheConfigsReadOnly;
    }

    @Nonnull
    @Override
    public MerkleTreeConfig getMerkleTreeConfig() {
        return merkleTreeConfigReadOnly;
    }

    @Nonnull
    @Override
    public EventJournalConfig getEventJournalConfig() {
        return eventJournalConfigReadOnly;
    }

    @Nonnull
    @Override
    public HotRestartConfig getHotRestartConfig() {
        return hotRestartConfigReadOnly;
    }

    @Nonnull
    @Override
    public DataPersistenceConfig getDataPersistenceConfig() {
        return dataPersistenceConfigReadOnly;
    }

    @Nonnull
    @Override
    public TieredStoreConfig getTieredStoreConfig() {
        return tieredStoreConfigReadOnly;
    }

    @Override
    public MapConfig setMerkleTreeConfig(@Nonnull MerkleTreeConfig merkleTreeConfig) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setEventJournalConfig(@Nonnull EventJournalConfig eventJournalConfig) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setHotRestartConfig(@Nonnull HotRestartConfig hotRestartConfig) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setName(String name) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setBackupCount(int backupCount) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setAsyncBackupCount(int asyncBackupCount) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setMaxIdleSeconds(int maxIdleSeconds) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setEvictionConfig(EvictionConfig evictionConfig) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setMapStoreConfig(MapStoreConfig mapStoreConfig) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setNearCacheConfig(NearCacheConfig nearCacheConfig) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setPerEntryStatsEnabled(boolean entryStatsEnabled) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setReadBackupData(boolean readBackupData) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setWanReplicationRef(WanReplicationRef wanReplicationRef) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig addEntryListenerConfig(EntryListenerConfig listenerConfig) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setEntryListenerConfigs(List<EntryListenerConfig> listenerConfigs) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setPartitioningStrategyConfig(PartitioningStrategyConfig partitioningStrategyConfig) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setPartitionLostListenerConfigs(List<MapPartitionLostListenerConfig> listenerConfigs) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setAttributeConfigs(List<AttributeConfig> attributeConfigs) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setQueryCacheConfigs(List<QueryCacheConfig> queryCacheConfigs) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setCacheDeserializedValues(CacheDeserializedValues cacheDeserializedValues) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig addIndexConfig(IndexConfig indexConfig) {
        throw throwReadOnly();
    }

    @Override
    public MapConfig setIndexConfigs(List<IndexConfig> indexConfigs) {
        throw throwReadOnly();
    }

    private UnsupportedOperationException throwReadOnly() {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
