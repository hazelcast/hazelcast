/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapConfigReadOnlyTest {

    private MapConfig getReadOnlyConfig() {
        return new MapConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getMaxSizeConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig();

        MaxSizeConfig maxSizeConfig = config.getAsReadOnly().getMaxSizeConfig();
        maxSizeConfig.setSize(2342);
    }

    @Test
    public void getMaxSizeConfigOfReadOnlyMapConfigShouldReturnNullIfConfigIsNull() {
        MapConfig config = new MapConfig()
                .setMaxSizeConfig(null);

        MaxSizeConfig maxSizeConfig = config.getAsReadOnly().getMaxSizeConfig();
        assertNull(maxSizeConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getWanReplicationRefOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setWanReplicationRef(new WanReplicationRef());

        WanReplicationRef wanReplicationRef = config.getAsReadOnly().getWanReplicationRef();
        wanReplicationRef.setName("myWanReplicationRef");
    }

    @Test
    public void getWanReplicationRefOfReadOnlyMapConfigShouldReturnNullIfWanReplicationRefIsNull() {
        MapConfig config = new MapConfig()
                .setWanReplicationRef(null);

        WanReplicationRef wanReplicationRef = config.getAsReadOnly().getWanReplicationRef();
        assertNull(wanReplicationRef);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getEntryListenerConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addEntryListenerConfig(new EntryListenerConfig())
                .addEntryListenerConfig(new EntryListenerConfig());

        List<EntryListenerConfig> listenerConfigs = config.getAsReadOnly().getEntryListenerConfigs();
        listenerConfigs.add(new EntryListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getPartitionLostListenerConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig())
                .addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig());

        List<MapPartitionLostListenerConfig> listenerConfigs = config.getAsReadOnly().getPartitionLostListenerConfigs();
        listenerConfigs.add(new MapPartitionLostListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getMapIndexConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addMapIndexConfig(new MapIndexConfig())
                .addMapIndexConfig(new MapIndexConfig());

        List<MapIndexConfig> mapIndexConfigs = config.getAsReadOnly().getMapIndexConfigs();
        mapIndexConfigs.add(new MapIndexConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getPartitioningStrategyConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setPartitioningStrategyConfig(new PartitioningStrategyConfig());

        PartitioningStrategyConfig partitioningStrategyConfig = config.getAsReadOnly().getPartitioningStrategyConfig();
        partitioningStrategyConfig.setPartitioningStrategy(null);
    }

    @Test
    public void getPartitioningStrategyConfigOfReadOnlyMapConfigShouldReturnNullIfConfigIsNull() {
        MapConfig config = new MapConfig()
                .setPartitioningStrategyConfig(null);

        PartitioningStrategyConfig partitioningStrategyConfig = config.getAsReadOnly().getPartitioningStrategyConfig();
        assertNull(partitioningStrategyConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getMapStoreConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setMapStoreConfig(new MapStoreConfig());

        MapStoreConfig mapStoreConfig = config.getAsReadOnly().getMapStoreConfig();
        mapStoreConfig.setEnabled(true);
    }

    @Test
    public void getMapStoreConfigOfReadOnlyMapConfigShouldReturnNullIfConfigIsNull() {
        MapConfig config = new MapConfig()
                .setMapStoreConfig(null);

        MapStoreConfig mapStoreConfig = config.getAsReadOnly().getMapStoreConfig();
        assertNull(mapStoreConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getNearCacheConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setNearCacheConfig(new NearCacheConfig());

        NearCacheConfig nearCacheConfig = config.getAsReadOnly().getNearCacheConfig();
        nearCacheConfig.setName("myNearCache");
    }

    @Test
    public void getNearCacheConfigOfReadOnlyMapConfigShouldReturnNullIfConfigIsNull() {
        MapConfig config = new MapConfig()
                .setNearCacheConfig(null);

        NearCacheConfig nearCacheConfig = config.getAsReadOnly().getNearCacheConfig();
        assertNull(nearCacheConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getQueryCacheConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addQueryCacheConfig(new QueryCacheConfig("myQueryCache1"))
                .addQueryCacheConfig(new QueryCacheConfig("myQueryCache2"));

        List<QueryCacheConfig> queryCacheConfigs = config.getAsReadOnly().getQueryCacheConfigs();
        queryCacheConfigs.add(new QueryCacheConfig("myQueryCache3"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setHotRestartConfigOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setHotRestartConfig(new HotRestartConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNameOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setName("myMap");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setInMemoryFormatOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setInMemoryFormat(InMemoryFormat.OBJECT);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setBackupCountOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setAsyncBackupCountOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setAsyncBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEvictionPercentageOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setEvictionPercentage(65);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMinEvictionCheckMillisOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setMinEvictionCheckMillis(23);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setTimeToLiveSecondsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setTimeToLiveSeconds(42);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMaxIdleSecondsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setMaxIdleSeconds(23);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMaxSizeConfigOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setMaxSizeConfig(new MaxSizeConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEvictionPolicyOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setEvictionPolicy(EvictionPolicy.NONE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMapEvictionPolicyOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setMapEvictionPolicy(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMapStoreConfigOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setMapStoreConfig(new MapStoreConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNearCacheConfigOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setNearCacheConfig(new NearCacheConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMergePolicyOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setMergePolicy("myMergePolicy");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setStatisticsEnabledOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setStatisticsEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setReadBackupDataOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setReadBackupData(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setWanReplicationRefOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setWanReplicationRef(new WanReplicationRef());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addEntryListenerConfigOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().addEntryListenerConfig(new EntryListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEntryListenerConfigsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setEntryListenerConfigs(singletonList(new EntryListenerConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addMapIndexConfigOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().addMapIndexConfig(new MapIndexConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMapIndexConfigsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setMapIndexConfigs(singletonList(new MapIndexConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setPartitioningStrategyConfigOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setPartitioningStrategyConfig(new PartitioningStrategyConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setOptimizeQueriesOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setOptimizeQueries(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setPartitionLostListenerConfigsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setPartitionLostListenerConfigs(singletonList(new MapPartitionLostListenerConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMapAttributeConfigsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setMapAttributeConfigs(singletonList(new MapAttributeConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setQueryCacheConfigsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setQueryCacheConfigs(singletonList(new QueryCacheConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setCacheDeserializedValuesOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setCacheDeserializedValues(CacheDeserializedValues.INDEX_ONLY);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setQuorumNameOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setQuorumName("myQuorum");
    }
}
