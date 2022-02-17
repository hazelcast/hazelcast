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

package com.hazelcast.config;

import com.hazelcast.internal.config.MapConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapConfigReadOnlyTest {

    private MapConfig getReadOnlyConfig() {
        return new MapConfigReadOnly(new MapConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getMaxSizeConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        new MapConfigReadOnly(new MapConfig()).getEvictionConfig().setSize(2342);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getWanReplicationRefOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setWanReplicationRef(new WanReplicationRef());

        WanReplicationRef wanReplicationRef = new MapConfigReadOnly(config).getWanReplicationRef();
        wanReplicationRef.setName("myWanReplicationRef");
    }

    @Test
    public void getWanReplicationRefOfReadOnlyMapConfigShouldReturnNullIfWanReplicationRefIsNull() {
        MapConfig config = new MapConfig()
                .setWanReplicationRef(null);

        WanReplicationRef wanReplicationRef = new MapConfigReadOnly(config).getWanReplicationRef();
        assertNull(wanReplicationRef);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getEntryListenerConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addEntryListenerConfig(new EntryListenerConfig())
                .addEntryListenerConfig(new EntryListenerConfig());

        List<EntryListenerConfig> listenerConfigs = new MapConfigReadOnly(config).getEntryListenerConfigs();
        listenerConfigs.add(new EntryListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getPartitionLostListenerConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig())
                .addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig());

        List<MapPartitionLostListenerConfig> listenerConfigs = new MapConfigReadOnly(config).getPartitionLostListenerConfigs();
        listenerConfigs.add(new MapPartitionLostListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getIndexConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addIndexConfig(new IndexConfig())
                .addIndexConfig(new IndexConfig());

        List<IndexConfig> indexConfigs = new MapConfigReadOnly(config).getIndexConfigs();
        indexConfigs.add(new IndexConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getPartitioningStrategyConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setPartitioningStrategyConfig(new PartitioningStrategyConfig());

        PartitioningStrategyConfig partitioningStrategyConfig = new MapConfigReadOnly(config).getPartitioningStrategyConfig();
        partitioningStrategyConfig.setPartitioningStrategy(null);
    }

    @Test
    public void getPartitioningStrategyConfigOfReadOnlyMapConfigShouldReturnNullIfConfigIsNull() {
        MapConfig config = new MapConfig()
                .setPartitioningStrategyConfig(null);

        PartitioningStrategyConfig partitioningStrategyConfig = new MapConfigReadOnly(config).getPartitioningStrategyConfig();
        assertNull(partitioningStrategyConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getMapStoreConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setMapStoreConfig(new MapStoreConfig());

        MapStoreConfig mapStoreConfig = new MapConfigReadOnly(config).getMapStoreConfig();
        mapStoreConfig.setEnabled(true);
    }

    @Test
    public void getMapStoreConfigOfReadOnlyMapConfigShouldReturnNullIfConfigIsNull() {
        MapConfig config = new MapConfig()
                .setMapStoreConfig(null);

        MapStoreConfig mapStoreConfig = new MapConfigReadOnly(config).getMapStoreConfig();
        assertNull(mapStoreConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getNearCacheConfigOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .setNearCacheConfig(new NearCacheConfig());

        NearCacheConfig nearCacheConfig = new MapConfigReadOnly(config).getNearCacheConfig();
        nearCacheConfig.setName("myNearCache");
    }

    @Test
    public void getNearCacheConfigOfReadOnlyMapConfigShouldReturnNullIfConfigIsNull() {
        MapConfig config = new MapConfig()
                .setNearCacheConfig(null);

        NearCacheConfig nearCacheConfig = new MapConfigReadOnly(config).getNearCacheConfig();
        assertNull(nearCacheConfig);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getQueryCacheConfigsOfReadOnlyMapConfigShouldReturnUnmodifiable() {
        MapConfig config = new MapConfig()
                .addQueryCacheConfig(new QueryCacheConfig("myQueryCache1"))
                .addQueryCacheConfig(new QueryCacheConfig("myQueryCache2"));

        List<QueryCacheConfig> queryCacheConfigs = new MapConfigReadOnly(config).getQueryCacheConfigs();
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
    public void setTimeToLiveSecondsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setTimeToLiveSeconds(42);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMaxIdleSecondsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setMaxIdleSeconds(23);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMaxSizeConfigOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().getEvictionConfig().setMaxSizePolicy(MaxSizePolicy.PER_PARTITION);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEvictionPolicyOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().getEvictionConfig().setEvictionPolicy(EvictionPolicy.NONE);
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
    public void addIndexConfigOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().addIndexConfig(new IndexConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setIndexConfigsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setIndexConfigs(singletonList(new IndexConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setPartitioningStrategyConfigOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setPartitioningStrategyConfig(new PartitioningStrategyConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setPartitionLostListenerConfigsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setPartitionLostListenerConfigs(singletonList(new MapPartitionLostListenerConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setAttributeConfigsOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setAttributeConfigs(singletonList(new AttributeConfig()));
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
    public void setSplitBrainProtectionNameOfReadOnlyMapConfigShouldFail() {
        getReadOnlyConfig().setSplitBrainProtectionName("mySplitBrainProtection");
    }
}
