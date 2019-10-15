/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryStore;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.mapstore.TestEntryStore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class WriteBehindEntryStoreQueueReplicationTest extends HazelcastTestSupport {

    @Test
    public void testWriteBehindQueueIsReplicatedWithExpirationTimes() {
        final int entryCount = 1000;
        final int ttlSec = 30;
        final String mapName = randomMapName();
        TestEntryStore<Integer, Integer> testEntryStore = new TestEntryStore<>();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(getConfigWithEntryStore(testEntryStore));
        IMap<Integer, Integer> map = instances[0].getMap(mapName);
        long expectedExpirationTime = System.currentTimeMillis() + ttlSec * 1000;
        for (int i = 0; i < 1000; i++) {
            map.put(i, i, ttlSec, TimeUnit.SECONDS);
        }
        factory.terminate(instances[0]);
        waitAllForSafeState(instances);
        factory.terminate(instances[1]);
        waitAllForSafeState(instances);

        IMap<Integer, Integer> mapFromSurvivingInstance = instances[2].getMap(mapName);

        // assert that entries are still stored in the entry store after original nodes crash
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                testEntryStore.assertRecordStored(i, i, expectedExpirationTime, 2000);
                assertEquals(i, (int) mapFromSurvivingInstance.get(i));
            }
        });

        // assert that entries still had expiration times and expired accordingly
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                assertNull(mapFromSurvivingInstance.get(i));
            }
        });
    }

    @Test
    public void testWriteBehindQueueIsReplicatedToNewInstancesWithExpirationTimes() {
        final int entryCount = 1000;
        final int ttlSec = 30;
        final String mapName = randomMapName();
        TestEntryStore<Integer, Integer> testEntryStore = new TestEntryStore<>();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = new HazelcastInstance[3];
        instances[0] = factory.newHazelcastInstance(getConfigWithEntryStore(testEntryStore));
        instances[1] = factory.newHazelcastInstance(getConfigWithEntryStore(testEntryStore));
        instances[2] = factory.newHazelcastInstance(getConfigWithEntryStore(testEntryStore));
        IMap<Integer, Integer> map = instances[0].getMap(mapName);
        long expectedExpirationTime = System.currentTimeMillis() + ttlSec * 1000;
        for (int i = 0; i < 1000; i++) {
            map.put(i, i, ttlSec, TimeUnit.SECONDS);
        }
        factory.terminate(instances[0]);
        waitAllForSafeState(instances);
        factory.terminate(instances[1]);
        waitAllForSafeState(instances);

        instances[0] = factory.newHazelcastInstance(getConfigWithEntryStore(testEntryStore));
        instances[1] = factory.newHazelcastInstance(getConfigWithEntryStore(testEntryStore));

        IMap<Integer, Integer> mapFromNewInstance = instances[2].getMap(mapName);

        // assert that entries are still stored in the entry store after original nodes crash
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                testEntryStore.assertRecordStored(i, i, expectedExpirationTime, 2000);
                assertEquals(i, (int) mapFromNewInstance.get(i));
            }
        });

        // assert that entries still had expiration times and expired accordingly
        assertTrueEventually(() -> {
            for (int i = 0; i < entryCount; i++) {
                assertNull(mapFromNewInstance.get(i));
            }
        });
    }

    private Config getConfigWithEntryStore(EntryStore entryStore) {
        Config config = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setWriteDelaySeconds(20).setImplementation(entryStore).setEnabled(true);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        return config;
    }
}
