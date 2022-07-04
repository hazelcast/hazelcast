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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.eviction.impl.comparator.LRUEvictionPolicyComparator;
import com.hazelcast.map.MapEvent;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class DynamicConfigBouncingTest extends HazelcastTestSupport {
    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(4)
            .driverCount(1)
            .useTerminate(true)
            .driverType(BounceTestConfiguration.DriverType.MEMBER)
            .build();

    public Config getConfig() {
        return new Config();
    }

    @Test
    public void doNotThrowExceptionWhenMemberIsGone() {
        Runnable[] methods = new Runnable[1];
        final String mapName = randomMapName();
        final HazelcastInstance testDriver = bounceMemberRule.getNextTestDriver();
        methods[0] = new SubmitDynamicMapConfig(mapName, testDriver);

        bounceMemberRule.testRepeatedly(methods, 60);
        HazelcastInstance instance = bounceMemberRule.getSteadyMember();
        MapConfig mapConfig = instance.getConfig().getMapConfig(mapName);
        assertEquals(createMapConfig(mapName), mapConfig);
    }

    private static MapConfig createMapConfig(String mapName) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setCacheLocalEntries(true)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE)
                .setPreloaderConfig(new NearCachePreloaderConfig()
                        .setEnabled(true));

        HotRestartConfig hotRestartConfig = new HotRestartConfig()
                .setEnabled(true)
                .setFsync(true);

        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig
                .setSize(1000)
                .setMaxSizePolicy(MaxSizePolicy.FREE_HEAP_SIZE)
                .setComparator(new LRUEvictionPolicyComparator());

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setClassName("foo.bar.MapStoreDoesNotExist");

        WanReplicationRef wanRef = new WanReplicationRef("name", "foo.bar.PolicyClass",
                Collections.<String>emptyList(), true);

        EntryListenerConfig classEntryListener = new EntryListenerConfig("foo.bar.ClassName", true, true);
        EntryListenerConfig entryListener = new EntryListenerConfig(new MyEntryListener(), true, true);
        EntryListenerConfig mapListener = new EntryListenerConfig(new MyEntryUpdatedListener(), true, true);

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig("queryCacheName")
                .setBatchSize(100)
                .addIndexConfig(new IndexConfig(IndexType.HASH, "attribute"))
                .addEntryListenerConfig(new EntryListenerConfig("foo.bar.Classname", false, true))
                .setInMemoryFormat(InMemoryFormat.OBJECT);

        return new MapConfig(mapName)
                .setBackupCount(2)
                .setBackupCount(3)
                .setTimeToLiveSeconds(12)
                .setMaxIdleSeconds(20)
                .setNearCacheConfig(nearCacheConfig)
                .setReadBackupData(true)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setHotRestartConfig(hotRestartConfig)
                .setEvictionConfig(evictionConfig)
                .setMapStoreConfig(mapStoreConfig)
                .setWanReplicationRef(wanRef)
                .addEntryListenerConfig(classEntryListener)
                .addEntryListenerConfig(entryListener)
                .addEntryListenerConfig(mapListener)
                .addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig("foo.bar.Classname"))
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "orderAttribute"))
                .addIndexConfig(new IndexConfig(IndexType.HASH, "unorderedAttribute"))
                .addAttributeConfig(new AttributeConfig("attribute", "foo.bar.ExtractorClass"))
                .addQueryCacheConfig(queryCacheConfig)
                .setStatisticsEnabled(false)
                .setPerEntryStatsEnabled(true)
                .setPartitioningStrategyConfig(new PartitioningStrategyConfig("foo.bar.Class"))
                .setSplitBrainProtectionName("split-brain-protection");
    }

    private static class MyEntryUpdatedListener implements EntryUpdatedListener, Serializable {

        @Override
        public void entryUpdated(EntryEvent event) {
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            return getClass().equals(obj.getClass());
        }
    }

    private static class MyEntryListener implements EntryListener, Serializable {

        @Override
        public void entryAdded(EntryEvent event) {
        }

        @Override
        public void entryUpdated(EntryEvent event) {
        }

        @Override
        public void entryRemoved(EntryEvent event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
        }

        @Override
        public void mapEvicted(MapEvent event) {
        }

        @Override
        public void entryEvicted(EntryEvent event) {
        }

        @Override
        public void entryExpired(EntryEvent event) {

        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            return getClass().equals(obj.getClass());
        }
    }

    private static class SubmitDynamicMapConfig implements Runnable {

        private final String mapName;
        private final HazelcastInstance testDriver;

        SubmitDynamicMapConfig(String mapName, HazelcastInstance testDriver) {
            this.mapName = mapName;
            this.testDriver = testDriver;
        }

        @Override
        public void run() {
            MapConfig mapConfig = createMapConfig(mapName);
            testDriver.getConfig().addMapConfig(mapConfig);
        }
    }
}
