/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindOnBackupsTest extends HazelcastTestSupport {

    /**
     * {@link com.hazelcast.map.impl.mapstore.writebehind.StoreWorker} delays processing of write-behind queues (wbq) by adding
     * delay with {@link com.hazelcast.instance.GroupProperties#PROP_MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS} property.
     * This is used to provide some extra robustness against node disaster scenarios by trying to prevent lost of entries in wbq-s.
     * Normally backup nodes don't store entries only remove them from wbq-s. Here, we are testing removal of entries occurred or not.
     */
    @Test
    public void testBackupRemovesEntries_afterProcessingDelay() throws Exception {
        final int numberOfItems = 10;
        final String mapName = randomMapName();
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        TestMapUsingMapStoreBuilder<Object, Object> storeBuilder = TestMapUsingMapStoreBuilder.create();
        final IMap<Object, Object> map = storeBuilder
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

    private void assertWriteBehindQueuesEmptyOnOwnerAndOnBackups(final String mapName,
                                                                 final long numberOfItems,
                                                                 final MapStoreWithCounter mapStore,
                                                                 final HazelcastInstance[] nodes) {
        AssertTask assertTask = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, writeBehindQueueSize(nodes[0], mapName));
                assertEquals(0, writeBehindQueueSize(nodes[1], mapName));

                assertEquals(numberOfItems, mapStore.size());
            }
        };

        assertTrueEventually(assertTask);
    }


    private void populateMap(IMap map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            map.put(i, i);
        }
    }

    private int writeBehindQueueSize(HazelcastInstance node, String mapName) {
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
            final MapDataStore<Data, Object> mapDataStore
                    = recordStore.getMapDataStore();
            size += ((WriteBehindStore) mapDataStore).getWriteBehindQueue().size();
        }
        return size;
    }
}
