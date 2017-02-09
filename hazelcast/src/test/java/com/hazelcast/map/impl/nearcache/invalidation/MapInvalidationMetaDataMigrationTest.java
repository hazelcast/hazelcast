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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapInvalidationMetaDataMigrationTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void sequences_migrated_whenNewlyJoinedNodesShutdown() throws Exception {
        String mapName = "test";
        Config config = newConfig(mapName);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = instance1.getMap(mapName);
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        Map<Integer, Long> source = getPartitionToSequenceMap(mapName, instance1);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        waitAllForSafeState(instance2);
        instance1.shutdown();

        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        waitAllForSafeState(instance3);
        instance2.shutdown();
        waitAllForSafeState(instance3);

        Map<Integer, Long> destination = getPartitionToSequenceMap(mapName, instance3);

        for (Map.Entry<Integer, Long> entry : source.entrySet()) {
            Integer key = entry.getKey();
            Long first = entry.getValue();
            Long last = destination.get(key);

            assertEquals(first, last);
        }
    }

    @Test
    public void sequences_migrated_whenSourceNodeShutdown() throws Exception {
        String mapName = "test";
        Config config = newConfig(mapName);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = instance1.getMap(mapName);
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        Map<Integer, Long> source1 = getPartitionToSequenceMap(mapName, instance1);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        waitAllForSafeState(instance2, instance3);

        instance1.shutdown();

        Map<Integer, Long> destination2 = getPartitionToSequenceMap(mapName, instance2);
        Map<Integer, Long> destination3 = getPartitionToSequenceMap(mapName, instance3);
        for (Map.Entry<Integer, Long> entry : destination2.entrySet()) {
            Integer key = entry.getKey();
            Long value = entry.getValue();
            if (value != 0) {
                destination3.put(key, value);
            }
        }

        assertEquals(source1, destination3);
    }

    @Test
    public void sequences_migrated_whenOneNodeContinuouslyStartsAndStops() throws Exception {
        final String mapName = "test";
        final Config config = newConfig(mapName);

        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = instance1.getMap(mapName);
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }
        Map<Integer, Long> source = getPartitionToSequenceMap(mapName, instance1);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final AtomicBoolean stop = new AtomicBoolean();

        Thread shadow = new Thread(new Runnable() {
            public void run() {
                while (!stop.get()) {
                    HazelcastInstance instance = factory.newHazelcastInstance(config);
                    waitAllForSafeState(instance);
                    sleepSeconds(5);
                    instance.shutdown();
                }
            }
        });

        shadow.start();
        sleepSeconds(20);
        stop.set(true);
        shadow.join();

        instance2.shutdown();

        Map<Integer, Long> destination = getPartitionToSequenceMap(mapName, instance1);

        assertEquals(source, destination);
    }

    @Test
    public void uuids_migrated_whenNewlyJoinedNodesShutdown() throws Exception {
        String mapName = "test";
        Config config = newConfig(mapName);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = instance1.getMap(mapName);
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        Map<Integer, UUID> source = getPartitionToUuidMap(instance1);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        waitAllForSafeState(instance2);
        instance1.shutdown();

        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        waitAllForSafeState(instance3);
        instance2.shutdown();

        Map<Integer, UUID> destination = getPartitionToUuidMap(instance3);


        assertEquals(source, destination);
    }

    @Test
    public void uuids_migrated_whenSourceNodeShutdown() throws Exception {
        String mapName = "test";
        Config config = newConfig(mapName);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = instance1.getMap(mapName);
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        Map<Integer, UUID> source1 = getPartitionToUuidMap(instance1);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        waitAllForSafeState(instance1, instance2, instance3);
        instance1.shutdown();

        Map<Integer, UUID> destination2 = getPartitionToUuidMap(instance2);
        Map<Integer, UUID> destination3 = getPartitionToUuidMap(instance3);

        Map<Integer, UUID> merged = mergeOwnedPartitionUuids(destination2, destination3,
                getNodeEngineImpl(instance2).getPartitionService());

        assertEquals(source1, merged);
    }

    protected Map<Integer, UUID> mergeOwnedPartitionUuids(Map<Integer, UUID> destination2, Map<Integer, UUID> destination3,
                                                          InternalPartitionService partitionService) {
        Map<Integer, UUID> merged = new HashMap<Integer, UUID>();

        int partitionCount = partitionService.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            if (partitionService.getPartition(i).isLocal()) {
                merged.put(i, destination2.get(i));
            } else {
                merged.put(i, destination3.get(i));
            }
        }
        return merged;
    }

    private Map<Integer, Long> getPartitionToSequenceMap(String mapName, HazelcastInstance instance) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        HashMap<Integer, Long> partitionToSequenceMap = new HashMap<Integer, Long>(partitionCount);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();

        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();
        for (int i = 0; i < partitionCount; i++) {
            partitionToSequenceMap.put(i, metaDataGenerator.currentSequence(mapName, i));
        }

        return partitionToSequenceMap;
    }

    private Map<Integer, UUID> getPartitionToUuidMap(HazelcastInstance instance) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        HashMap<Integer, UUID> partitionToSequenceMap = new HashMap<Integer, UUID>(partitionCount);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();

        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();
        for (int i = 0; i < partitionCount; i++) {
            partitionToSequenceMap.put(i, metaDataGenerator.getUuidOrNull(i));
        }

        return partitionToSequenceMap;
    }

    protected Config newConfig(String mapName) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(getNearCacheInMemoryFormat());
        nearCacheConfig.setName(mapName);
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setCacheLocalEntries(true);

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setNearCacheConfig(nearCacheConfig);
        mapConfig.setBackupCount(0).setAsyncBackupCount(0);

        Config config = getConfig();
        return config.addMapConfig(mapConfig);
    }

    protected InMemoryFormat getNearCacheInMemoryFormat() {
        return BINARY;
    }
}
