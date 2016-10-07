/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapConfigReadOnlyTest {

    private MapConfigReadOnly getMapConfigReadOnly() {
        return new MapConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingMaxSizeConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig();

        MaxSizeConfig maxSizeConfig = config.getAsReadOnly().getMaxSizeConfig();
        maxSizeConfig.setSize(2342);
    }

    @Test
    public void gettingMaxSizeConfigOfReadOnlyMapConfigShouldReturnNullIfConfigIsNull() {
        MapConfig config = new MapConfig()
                .setMaxSizeConfig(null);

        MaxSizeConfig maxSizeConfig = config.getAsReadOnly().getMaxSizeConfig();
        assertNull(maxSizeConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingWanReplicationRefOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setWanReplicationRef(new WanReplicationRef());

        WanReplicationRef wanReplicationRef = config.getAsReadOnly().getWanReplicationRef();
        wanReplicationRef.setName("myWanReplicationRef");
    }

    @Test
    public void gettingWanReplicationRefOfReadOnlyMapConfigShouldReturnNullIfWanReplicationRefIsNull() {
        MapConfig config = new MapConfig()
                .setWanReplicationRef(null);

        WanReplicationRef wanReplicationRef = config.getAsReadOnly().getWanReplicationRef();
        assertNull(wanReplicationRef);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingEntryListenerConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addEntryListenerConfig(new EntryListenerConfig())
                .addEntryListenerConfig(new EntryListenerConfig());

        List<EntryListenerConfig> listenerConfigs = config.getAsReadOnly().getEntryListenerConfigs();
        listenerConfigs.add(new EntryListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingPartitionLostListenerConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig())
                .addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig());

        List<MapPartitionLostListenerConfig> listenerConfigs = config.getAsReadOnly().getPartitionLostListenerConfigs();
        listenerConfigs.add(new MapPartitionLostListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingMapIndexConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addMapIndexConfig(new MapIndexConfig())
                .addMapIndexConfig(new MapIndexConfig());

        List<MapIndexConfig> mapIndexConfigs = config.getAsReadOnly().getMapIndexConfigs();
        mapIndexConfigs.add(new MapIndexConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingPartitioningStrategyConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setPartitioningStrategyConfig(new PartitioningStrategyConfig());

        PartitioningStrategyConfig partitioningStrategyConfig = config.getAsReadOnly().getPartitioningStrategyConfig();
        partitioningStrategyConfig.setPartitionStrategy(null);
    }

    @Test
    public void gettingPartitioningStrategyConfigOfReadOnlyMapConfigShouldReturnNullIfConfigIsNull() {
        MapConfig config = new MapConfig()
                .setPartitioningStrategyConfig(null);

        PartitioningStrategyConfig partitioningStrategyConfig = config.getAsReadOnly().getPartitioningStrategyConfig();
        assertNull(partitioningStrategyConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingMapStoreConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setMapStoreConfig(new MapStoreConfig());

        MapStoreConfig mapStoreConfig = config.getAsReadOnly().getMapStoreConfig();
        mapStoreConfig.setEnabled(true);
    }

    @Test
    public void gettingMapStoreConfigOfReadOnlyMapConfigShouldReturnNullIfConfigIsNull() {
        MapConfig config = new MapConfig()
                .setMapStoreConfig(null);

        MapStoreConfig mapStoreConfig = config.getAsReadOnly().getMapStoreConfig();
        assertNull(mapStoreConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingNearCacheConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setNearCacheConfig(new NearCacheConfig());

        NearCacheConfig nearCacheConfig = config.getAsReadOnly().getNearCacheConfig();
        nearCacheConfig.setName("myNearCache");
    }

    @Test
    public void gettingNearCacheConfigOfReadOnlyMapConfigShouldReturnNullIfConfigIsNull() {
        MapConfig config = new MapConfig()
                .setNearCacheConfig(null);

        NearCacheConfig nearCacheConfig = config.getAsReadOnly().getNearCacheConfig();
        assertNull(nearCacheConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingQueryCacheConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addQueryCacheConfig(new QueryCacheConfig("myQueryCache1"))
                .addQueryCacheConfig(new QueryCacheConfig("myQueryCache2"));

        List<QueryCacheConfig> queryCacheConfigs = config.getAsReadOnly().getQueryCacheConfigs();
        queryCacheConfigs.add(new QueryCacheConfig("myQueryCache3"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingHotRestartConfigOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setHotRestartConfig(new HotRestartConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingNameOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setName("myMap");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingInMemoryFormatOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setInMemoryFormat(InMemoryFormat.OBJECT);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingBackupCountOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingAsyncBackupCountOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setAsyncBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingEvictionPercentageOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setEvictionPercentage(65);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMinEvictionCheckMillisOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setMinEvictionCheckMillis(23);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingTimeToLiveSecondsOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setTimeToLiveSeconds(42);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMaxIdleSecondsOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setMaxIdleSeconds(23);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMaxSizeConfigOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setMaxSizeConfig(new MaxSizeConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingEvictionPolicyOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setEvictionPolicy(EvictionPolicy.NONE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMapEvictionPolicyOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setMapEvictionPolicy(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMapStoreConfigOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setMapStoreConfig(new MapStoreConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingNearCacheConfigOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setNearCacheConfig(new NearCacheConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMergePolicyOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setMergePolicy("myMergePolicy");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingStatisticsEnabledOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setStatisticsEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingReadBackupDataOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setReadBackupData(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingWanReplicationRefOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setWanReplicationRef(new WanReplicationRef());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addingEntryListenerConfigOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().addEntryListenerConfig(new EntryListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingEntryListenerConfigsOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setEntryListenerConfigs(singletonList(new EntryListenerConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addingMapIndexConfigOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().addMapIndexConfig(new MapIndexConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMapIndexConfigsOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setMapIndexConfigs(singletonList(new MapIndexConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingPartitioningStrategyConfigOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setPartitioningStrategyConfig(new PartitioningStrategyConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingOptimizeQueriesOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setOptimizeQueries(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingPartitionLostListenerConfigsOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setPartitionLostListenerConfigs(singletonList(new MapPartitionLostListenerConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingMapAttributeConfigsOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setMapAttributeConfigs(singletonList(new MapAttributeConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingQueryCacheConfigsOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setQueryCacheConfigs(singletonList(new QueryCacheConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingCacheDeserializedValuesOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setCacheDeserializedValues(CacheDeserializedValues.INDEX_ONLY);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingQuorumNameOfReadOnlyMapConfigShouldFail() {
        getMapConfigReadOnly().setQuorumName("myQuorum");
    }
}
