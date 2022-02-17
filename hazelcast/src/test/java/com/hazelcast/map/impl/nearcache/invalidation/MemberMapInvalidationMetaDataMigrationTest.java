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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberMapInvalidationMetaDataMigrationTest extends HazelcastTestSupport {

    private static final int MAP_SIZE = 10000;
    private static final String MAP_NAME = "MapInvalidationMetaDataMigrationTest";

    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
    private Config config;

    @Before
    public void setUp() {
        config = getConfig(MAP_NAME);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void sequences_migrated_when_newly_joined_nodes_shutdown() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        assertInvalidationCountEventually(MAP_NAME, MAP_SIZE, instance1);

        Map<Integer, Long> source = getPartitionToSequenceMap(MAP_NAME, instance1);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        instance1.shutdown();

        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        instance2.shutdown();

        waitAllForSafeState(factory.getAllHazelcastInstances());

        Map<Integer, Long> destination = getPartitionToSequenceMap(MAP_NAME, instance3);

        assertSequenceNumbersEqual(source, destination);
    }

    @Test
    public void sequences_migrated_when_source_node_shutdown() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        assertInvalidationCountEventually(MAP_NAME, MAP_SIZE, instance1);

        Map<Integer, Long> source = getPartitionToSequenceMap(MAP_NAME, instance1);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        instance1.shutdown();

        waitAllForSafeState(factory.getAllHazelcastInstances());

        Map<Integer, Long> destination2 = getPartitionToSequenceMap(MAP_NAME, instance2);
        Map<Integer, Long> destination3 = getPartitionToSequenceMap(MAP_NAME, instance3);

        for (Map.Entry<Integer, Long> entry : destination2.entrySet()) {
            Integer key = entry.getKey();
            Long value = entry.getValue();
            if (value != 0) {
                destination3.put(key, value);
            }
        }

        assertSequenceNumbersEqual(source, destination3);
    }

    @Test
    @Category(SlowTest.class)
    public void sequences_migrated_when_one_node_continuously_starts_and_stops() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        assertInvalidationCountEventually(MAP_NAME, MAP_SIZE, instance1);

        Map<Integer, Long> source = getPartitionToSequenceMap(MAP_NAME, instance1);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        final AtomicBoolean stop = new AtomicBoolean();
        Thread shadow = new Thread(new Runnable() {
            @Override
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
        assertJoinable(shadow);

        instance2.shutdown();

        waitAllForSafeState(factory.getAllHazelcastInstances());

        Map<Integer, Long> destination = getPartitionToSequenceMap(MAP_NAME, instance1);

        assertSequenceNumbersEqual(source, destination);
    }

    @Test
    public void uuids_migrated_when_newly_joined_nodes_shutdown() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        assertInvalidationCountEventually(MAP_NAME, MAP_SIZE, instance1);

        Map<Integer, UUID> source = getPartitionToUuidMap(instance1);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        instance1.shutdown();

        HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        instance2.shutdown();

        waitAllForSafeState(factory.getAllHazelcastInstances());
        Map<Integer, UUID> destination = getPartitionToUuidMap(instance3);
        assertPartitionUUIDsEqual(source, destination);
    }

    @Test
    public void uuids_migrated_when_source_node_shutdown() {
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            map.put(i, i);
        }

        assertInvalidationCountEventually(MAP_NAME, MAP_SIZE, instance1);

        Map<Integer, UUID> source = getPartitionToUuidMap(instance1);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        instance1.shutdown();

        waitAllForSafeState(factory.getAllHazelcastInstances());

        Map<Integer, UUID> destination2 = getPartitionToUuidMap(instance2);
        Map<Integer, UUID> destination3 = getPartitionToUuidMap(instance3);

        InternalPartitionService partitionService2 = getNodeEngineImpl(instance2).getPartitionService();
        Map<Integer, UUID> merged = mergeOwnedPartitionUuids(partitionService2, destination2, destination3);
        assertPartitionUUIDsEqual(source, merged);
    }

    private void assertInvalidationCountEventually(final String mapName, final int expectedInvalidationCount,
                                                   final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                long invalidationCount = calculateNumberOfInvalidationsSoFar(mapName, instance);
                assertEquals(expectedInvalidationCount, invalidationCount);
            }
        });
    }

    protected Config getConfig(String mapName) {
        MapConfig mapConfig = getMapConfig(mapName);

        return getBaseConfig()
                .addMapConfig(mapConfig);
    }

    protected MapConfig getMapConfig(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);

        return new MapConfig(mapName)
                .setNearCacheConfig(nearCacheConfig)
                .setBackupCount(0)
                .setAsyncBackupCount(0);
    }

    protected NearCacheConfig getNearCacheConfig(String mapName) {
        return new NearCacheConfig(mapName)
                .setInMemoryFormat(BINARY)
                .setInvalidateOnChange(true)
                .setCacheLocalEntries(true);
    }

    private static long calculateNumberOfInvalidationsSoFar(String mapName, HazelcastInstance instance) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        MetaDataGenerator metaDataGenerator = getMetaDataGenerator(nodeEngineImpl);

        long invalidationCount = 0;
        for (int i = 0; i < partitionCount; i++) {
            invalidationCount += metaDataGenerator.currentSequence(mapName, i);
        }
        return invalidationCount;
    }

    private static Map<Integer, Long> getPartitionToSequenceMap(String mapName, HazelcastInstance instance) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        MetaDataGenerator metaDataGenerator = getMetaDataGenerator(nodeEngineImpl);

        Map<Integer, Long> partitionToSequenceMap = createHashMap(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionToSequenceMap.put(i, metaDataGenerator.currentSequence(mapName, i));
        }
        return partitionToSequenceMap;
    }

    private static Map<Integer, UUID> getPartitionToUuidMap(HazelcastInstance instance) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        MetaDataGenerator metaDataGenerator = getMetaDataGenerator(nodeEngineImpl);

        Map<Integer, UUID> partitionToUuidMap = createHashMap(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionToUuidMap.put(i, metaDataGenerator.getUuidOrNull(i));
        }
        return partitionToUuidMap;
    }

    private static MetaDataGenerator getMetaDataGenerator(NodeEngineImpl nodeEngineImpl) {
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        return invalidator.getMetaDataGenerator();
    }

    private static Map<Integer, UUID> mergeOwnedPartitionUuids(InternalPartitionService localPartitionService,
                                                               Map<Integer, UUID> localUUIDs, Map<Integer, UUID> remoteUUIDs) {
        int partitionCount = localPartitionService.getPartitionCount();
        Map<Integer, UUID> merged = createHashMap(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            if (localPartitionService.getPartition(i).isLocal()) {
                merged.put(i, localUUIDs.get(i));
            } else {
                merged.put(i, remoteUUIDs.get(i));
            }
        }
        return merged;
    }

    private static void assertSequenceNumbersEqual(Map<Integer, Long> source, Map<Integer, Long> destination) {
        for (Map.Entry<Integer, Long> entry : source.entrySet()) {
            Integer key = entry.getKey();
            Long first = entry.getValue();
            Long last = destination.get(key);

            assertEquals(format(
                    "Expected source and destination sequence numbers to be the same (source: %s) (destination %s)",
                    source, destination),
                    first, last);
        }
    }

    private static void assertPartitionUUIDsEqual(Map<Integer, UUID> source, Map<Integer, UUID> destination) {
        for (Map.Entry<Integer, UUID> entry : source.entrySet()) {
            Integer key = entry.getKey();
            UUID first = entry.getValue();
            UUID last = destination.get(key);

            assertEquals(format(
                    "Expected source and destination partition UUIDs to be the same (source: %s) (destination %s)",
                    source, destination),
                    first, last);
        }
    }
}
