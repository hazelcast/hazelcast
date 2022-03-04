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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindOnBackupsTest extends HazelcastTestSupport {

    /**
     * {@link com.hazelcast.map.impl.mapstore.writebehind.StoreWorker} delays processing of write-behind queues (wbq) by adding
     * delay with {@link ClusterProperty#MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS} property.
     * This is used to provide some extra robustness against node disaster scenarios by trying to prevent lost of entries in wbq-s.
     * Normally backup nodes don't store entries only remove them from wbq-s. Here, we are testing removal of entries occurred or not.
     */
    @Test
    public void testBackupRemovesEntries_afterProcessingDelay() throws Exception {
        final int numberOfItems = 10;
        final String mapName = randomMapName();
        final MapStoreWithCounter<Integer, Integer> mapStore = new MapStoreWithCounter<Integer, Integer>();
        TestMapUsingMapStoreBuilder<Integer, Integer> storeBuilder = TestMapUsingMapStoreBuilder.create();
        final IMap<Integer, Integer> map = storeBuilder
                .mapName(mapName)
                .withMapStore(mapStore)
                .withNodeCount(2)
                .withNodeFactory(createHazelcastInstanceFactory(2))
                .withWriteDelaySeconds(1)
                .withBackupCount(1)
                .withPartitionCount(1)
                .withBackupProcessingDelay(1)
                .build();

        populateMap(map, numberOfItems);

        assertWriteBehindQueuesEmptyOnOwnerAndOnBackups(mapName, numberOfItems, mapStore, storeBuilder.getNodes());
    }

    @Test
    public void testPutTransientDoesNotStoreEntry_onBackupPartition() {
        String mapName = randomMapName();
        final MapStoreWithCounter<Integer, Integer> mapStore = new MapStoreWithCounter<Integer, Integer>();
        TestMapUsingMapStoreBuilder<Integer, Integer> storeBuilder = TestMapUsingMapStoreBuilder.create();
        final IMap<Integer, Integer> map = storeBuilder
                .mapName(mapName)
                .withMapStore(mapStore)
                .withNodeCount(2)
                .withNodeFactory(createHazelcastInstanceFactory(2))
                .withWriteDelaySeconds(1)
                .withBackupCount(1)
                .withPartitionCount(1)
                .withBackupProcessingDelay(1)
                .build();

        map.putTransient(1, 1, 1, TimeUnit.DAYS);

        sleepSeconds(5);

        assertEquals("There should not be any store operation", 0, mapStore.countStore.get());
    }

    @Test
    @Category(SlowTest.class)
    public void testPutTransientDoesNotStoreEntry_onPromotedReplica() {
        String mapName = randomMapName();
        final MapStoreWithCounter<String, Object> mapStore = new MapStoreWithCounter<String, Object>();
        TestMapUsingMapStoreBuilder<String, Object> storeBuilder = TestMapUsingMapStoreBuilder.create();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final IMap<String, Object> map = storeBuilder
                .mapName(mapName)
                .withMapStore(mapStore)
                .withNodeCount(2)
                .withNodeFactory(factory)
                .withWriteDelaySeconds(5)
                .withBackupCount(1)
                .withPartitionCount(1)
                .withBackupProcessingDelay(1)
                .build();

        String key = UUID.randomUUID().toString();

        map.putTransient(key, 1, 1, TimeUnit.DAYS);

        killKeyOwner(key, storeBuilder);

        sleepSeconds(10);

        assertEquals("There should not be any store operation on promoted replica", 0, mapStore.countStore.get());
    }

    private void killKeyOwner(String key, TestMapUsingMapStoreBuilder<String, Object> storeBuilder) {
        HazelcastInstance[] nodes = storeBuilder.getNodes();
        HazelcastInstance ownerNode = getOwnerNode(key, nodes);
        ownerNode.shutdown();
    }

    private HazelcastInstance getOwnerNode(String key, HazelcastInstance[] nodes) {
        PartitionService partitionService = nodes[0].getPartitionService();
        Partition partition = partitionService.getPartition(key);
        Member owner = partition.getOwner();

        for (HazelcastInstance node : nodes) {
            Member localMember = node.getCluster().getLocalMember();
            if (localMember.equals(owner)) {
                return node;
            }
        }

        throw new IllegalStateException("This should not be happen...");
    }

    private void assertWriteBehindQueuesEmptyOnOwnerAndOnBackups(final String mapName,
                                                                 final long numberOfItems,
                                                                 final MapStoreWithCounter mapStore,
                                                                 final HazelcastInstance[] nodes) {
        AssertTask assertTask = () -> {
            assertEquals(0, writeBehindQueueSize(nodes[0], mapName));
            assertEquals(0, writeBehindQueueSize(nodes[1], mapName));

            assertEquals(numberOfItems, mapStore.size());
        };

        assertTrueEventually(assertTask);
    }

    private void populateMap(IMap<Integer, Integer> map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            map.put(i, i);
        }
    }

    public static int writeBehindQueueSize(HazelcastInstance node, String mapName) {
        int size = 0;
        final NodeEngineImpl nodeEngine = getNode(node).getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            final RecordStore recordStore = mapServiceContext.getExistingRecordStore(i, mapName);
            if (recordStore == null) {
                continue;
            }
            final MapDataStore mapDataStore = recordStore.getMapDataStore();
            if (mapDataStore instanceof WriteBehindStore) {
                size += ((WriteBehindStore) mapDataStore).getWriteBehindQueue().size();
            }
        }
        return size;
    }
}
