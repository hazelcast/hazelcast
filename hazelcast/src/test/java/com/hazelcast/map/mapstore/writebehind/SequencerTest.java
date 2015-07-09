/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SequencerTest extends HazelcastTestSupport {

    static {
        System.setProperty("hazelcast.test.disableThreadDumpOnFailure", "true");
    }

    @Test
    public void testLastSequenceIsSameOnOwnerAndBackup() throws Exception {
        String mapName = randomMapName();
        int nodeCount = 2;
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        TestMapUsingMapStoreBuilder builder = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(nodeCount)
                .withNodeFactory(createHazelcastInstanceFactory(nodeCount))
                .withBackupCount(1)
                .withPartitionCount(1)
                .withWriteCoalescing(true)
                .mapName(mapName)
                .withWriteDelaySeconds(10);

        IMap<Object, Object> map = builder.build();
        HazelcastInstance[] nodes = builder.getNodes();

        int populationCount = 1;
        for (int i = 0; i < populationCount; i++) {
            map.put(i, randomString());
        }

        long lastSequenceOnNode1 = getLastSequence(mapName, nodes[0].getMap(mapName));
        long lastSequenceOnNode2 = getLastSequence(mapName, nodes[1].getMap(mapName));

        assertEquals(populationCount, lastSequenceOnNode1);
        assertEquals(populationCount, lastSequenceOnNode2);
    }

    @Test
    public void testSequenceContinuesToGrow_afterManyNodeStartStops_whenCoalescingDisabled() throws Exception {
        int nodeCount = 3;
        String mapName = randomMapName();
        MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        Config config = getConfig(mapName, mapStore, false);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        int popCount = 1;
        HazelcastInstance node1 = populateAndGetNode(mapName, popCount, config, factory);

        HazelcastInstance node2 = populateAndGetNode(mapName, popCount, config, factory);

        long lastSequence1 = getLastSequence(mapName, node1.getMap(mapName));
        long lastSequence2 = getLastSequence(mapName, node2.getMap(mapName));

//        node2.shutdown();
//
//        HazelcastInstance node3 = populateAndGetNode(mapName, popCount, config, factory);
//
//        long lastSequence3 = getLastSequence(mapName, node3.getMap(mapName));

        assertEquals(2 * popCount, lastSequence1);
        assertEquals(2 * popCount, lastSequence2);
//        assertEquals(popCount, lastSequence3);
    }


    @Test
    public void testSequenceContinuesToGrow_afterManyNodeStartStops_whenCoalescingEnabled() throws Exception {
        int nodeCount = 3;
        String mapName = randomMapName();
        MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        Config config = getConfig(mapName, mapStore, true);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        int popCount = 1453;
        HazelcastInstance node1 = populateAndGetNode(mapName, popCount, config, factory);

        HazelcastInstance node2 = populateAndGetNode(mapName, popCount, config, factory);

        long lastSequence1 = getLastSequence(mapName, node1.getMap(mapName));
        long lastSequence2 = getLastSequence(mapName, node2.getMap(mapName));

//        node2.shutdown();
//
//        HazelcastInstance node3 = populateAndGetNode(mapName, popCount, config, factory);
//
//        long lastSequence3 = getLastSequence(mapName, node3.getMap(mapName));

        assertEquals(popCount, lastSequence1);
        assertEquals(popCount, lastSequence2);
//        assertEquals(popCount, lastSequence3);
    }

    private HazelcastInstance populateAndGetNode(String mapName, int count, Config config, TestHazelcastInstanceFactory factory) {
        HazelcastInstance node = factory.newHazelcastInstance(config);
        for (int i = 0; i < count; i++) {
            node.getMap(mapName).put(i, i);
        }
        return node;
    }

    private Config getConfig(String mapName, MapStoreWithCounter mapStore, boolean coalescingOn) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig
                .setImplementation(mapStore)
                .setWriteDelaySeconds(2)
                .setWriteCoalescing(coalescingOn);

        Config config = new Config();
        config.getMapConfig(mapName)
                .setBackupCount(1)
                .setMapStoreConfig(mapStoreConfig);

        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1");
        return config;
    }

    private long getLastSequence(String mapName, Map map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapService service = (MapService) mapProxy.getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(0);
        RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
        if (recordStore == null) {
            return 0L;
        }
        return ((WriteBehindStore) recordStore.getMapDataStore()).getWriteBehindQueue()
                .getSequencer().tailSequence();
    }

}
