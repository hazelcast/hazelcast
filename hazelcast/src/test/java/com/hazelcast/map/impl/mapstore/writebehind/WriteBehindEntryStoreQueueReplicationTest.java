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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryStore;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.TestEntryStore;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindEntryStoreQueueReplicationTest extends HazelcastTestSupport {

    @Test
    public void queued_entries_with_expiration_times_are_not_lost_when_cluster_scaled_down() {
        final int entryCount = 1000;
        final int ttlSec = 10;
        final int writeDelaySec = 5;
        final int backupCount = 1;
        final String mapName = randomMapName();

        TestEntryStore<Integer, Integer> testEntryStore = new TestEntryStore<>();
        Config config = getConfigWithEntryStore(testEntryStore, writeDelaySec, backupCount);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);

        Map<Integer, Long> expectedExpiryTimes = new HashMap<>();
        IMap<Integer, Integer> map = instances[0].getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i, ttlSec, TimeUnit.SECONDS);
            expectedExpiryTimes.put(i, map.getEntryView(i).getExpirationTime());
        }

        // scale down
        instances[0].shutdown();
        instances[1].shutdown();

        IMap<Integer, Integer> mapFromSurvivingInstance = instances[2].getMap(mapName);

        // assert that entries are still stored in
        // the entry store after original nodes crash
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                testEntryStore.assertRecordStored(i, i, expectedExpiryTimes.get(i), 2000);
            }
        });

        // assert that entries still had
        // expiration times and expired accordingly
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                assertNull(dumpNotExpiredRecordsToString(instances[2], mapName),
                        mapFromSurvivingInstance.get(i));
            }
        }, 240);
    }

    private static String dumpNotExpiredRecordsToString(HazelcastInstance node, String mapName) {
        List<Long> msg = new ArrayList<>();
        NodeEngineImpl nodeEngine = getNode(node).getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            RecordStore<Record> recordStore = mapServiceContext.getExistingRecordStore(i, mapName);
            if (recordStore == null) {
                continue;
            }
            recordStore.forEach((data, record) -> msg.add(recordStore.getExpirySystem()
                    .getExpiryMetadata(data).getTtl()), false);
        }
        return msg.toString();
    }

    @Test
    public void queued_entries_with_expirationTimes_are_replicated_when_cluster_scaled_up() {
        final int entryCount = 1000;
        final int ttlSec = 10;
        final int writeDelaySec = 5;
        final int backupCount = 1;
        final String mapName = randomMapName();

        TestEntryStore<Integer, Integer> testEntryStore = new TestEntryStore<>();
        Config config = getConfigWithEntryStore(testEntryStore, writeDelaySec, backupCount);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance node1 = factory.newHazelcastInstance(config);

        Map<Integer, Long> expectedExpiryTimes = new HashMap<>();
        IMap<Integer, Integer> map = node1.getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i, ttlSec, TimeUnit.SECONDS);
            expectedExpiryTimes.put(i, map.getEntryView(i).getExpirationTime());
        }

        // scale up
        HazelcastInstance node2 = factory.newHazelcastInstance(config);
        HazelcastInstance node3 = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> mapFromNewInstance = node3.getMap(mapName);

        // assert that entries are still stored in
        // the entry store after original nodes crash
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                testEntryStore.assertRecordStored(i, i, expectedExpiryTimes.get(i), 2000);
            }
        });

        // assert that entries still had
        // expiration times and expired accordingly
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                assertNull(dumpNotExpiredRecordsToString(node3, mapName),
                        mapFromNewInstance.get(i));
            }
        }, 240);
    }

    private Config getConfigWithEntryStore(EntryStore entryStore,
                                           int writeDelaySeconds, int backupCount) {
        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig
                .setEnabled(true)
                .setWriteDelaySeconds(writeDelaySeconds)
                .setImplementation(entryStore);
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setBackupCount(backupCount);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return config;
    }
}
