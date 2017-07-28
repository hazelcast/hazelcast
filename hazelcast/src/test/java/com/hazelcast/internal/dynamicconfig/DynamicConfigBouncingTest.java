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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.eviction.LFUEvictionPolicy;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelTest.class})
public class DynamicConfigBouncingTest extends HazelcastTestSupport {
    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(4)
            .driverCount(1)
            .useTerminate()
            .build();


    public Config getConfig() {
        return new Config();
    }

    @Test
    public void doNotThrowExceptionWhenMemberIsGone() throws Exception {
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
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.getHotRestartConfig().setEnabled(true);
        mapConfig.getHotRestartConfig().setFsync(true);

        mapConfig.setBackupCount(2);
        mapConfig.setBackupCount(3);

        mapConfig.setTimeToLiveSeconds(12);
        mapConfig.setMaxIdleSeconds(20);

        mapConfig.getMaxSizeConfig().setSize(1000);
        mapConfig.getMaxSizeConfig().setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.FREE_HEAP_SIZE);

        mapConfig.setMapEvictionPolicy(new LFUEvictionPolicy());

        mapConfig.getMapStoreConfig().setEnabled(true);
        mapConfig.getMapStoreConfig().setClassName("foo.bar.MapStoreDoesNotExist");

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        nearCacheConfig.setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE);
        nearCacheConfig.getPreloaderConfig().setEnabled(true);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        mapConfig.setReadBackupData(true);
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.ALWAYS);
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);

        WanReplicationRef wanRef = new WanReplicationRef("name", "foo.bar.PolicyClass",
                Collections.<String>emptyList(), true);
        mapConfig.setWanReplicationRef(wanRef);

        EntryListenerConfig classEntryListener = new EntryListenerConfig("foo.bar.ClassName", true, true);
        EntryListenerConfig entryListener = new EntryListenerConfig(new MyEntryListener(), true, true);
        EntryListenerConfig mapListener = new EntryListenerConfig(new MyEntryUpdatedListener(), true, true);
        mapConfig.addEntryListenerConfig(classEntryListener);
        mapConfig.addEntryListenerConfig(entryListener);
        mapConfig.addEntryListenerConfig(mapListener);

        mapConfig.addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig("foo.bar.Classname"));
        mapConfig.addMapIndexConfig(new MapIndexConfig("orderAttribute", true));
        mapConfig.addMapIndexConfig(new MapIndexConfig("unorderedAttribute", false));

        mapConfig.addMapAttributeConfig(new MapAttributeConfig("attribute", "foo.bar.ExtractorClass"));

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig("queryCacheName");
        queryCacheConfig.setBatchSize(100);
        queryCacheConfig.addIndexConfig(new MapIndexConfig("attribute", false));
        queryCacheConfig.addEntryListenerConfig(new EntryListenerConfig("foo.bar.Classname", false, true));
        queryCacheConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        mapConfig.addQueryCacheConfig(queryCacheConfig);

        mapConfig.setStatisticsEnabled(false);

        mapConfig.setPartitioningStrategyConfig(new PartitioningStrategyConfig("foo.bar.Class"));
        mapConfig.setQuorumName("quorum");

        return mapConfig;
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

        public SubmitDynamicMapConfig(String mapName, HazelcastInstance testDriver) {
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
