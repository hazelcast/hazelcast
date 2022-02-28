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

package com.hazelcast.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.hazelcast.multimap.MultiMapTestUtil.getBackupMultiMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMapBackupTest extends HazelcastTestSupport {

    private static final String MULTI_MAP_NAME = "MultiMapBackupTest";
    private static final int KEY_COUNT = 1000;
    private static final int VALUE_COUNT = 5;
    private static final int BACKUP_COUNT = 4;

    private static final ILogger LOGGER = Logger.getLogger(MultiMapBackupTest.class);

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Test
    public void testBackupsPutAll() {
        Consumer<MultiMap<Integer, Integer>> c = (o) -> {
            Map<Integer, Collection<? extends Integer>> expectedMultiMap = new HashMap<>();
            for (int i = 0; i < KEY_COUNT; i++) {
                Collection<Integer> coll = new ArrayList<>();
                for (int j = 0; j < VALUE_COUNT; j++) {
                    coll.add(i + j);
                }
                expectedMultiMap.put(i, coll);
            }
            o.putAllAsync(expectedMultiMap);
        };
        testBackupsTemplate(MULTI_MAP_NAME, 3, 3, KEY_COUNT, VALUE_COUNT, c);
    }

    @Test
    public void testBackupsPut() {
        Consumer<MultiMap<Integer, Integer>> c = (o) -> {
            for (int i = 0; i < KEY_COUNT; i++) {
                for (int j = 0; j < VALUE_COUNT; j++) {
                    o.put(i, i + j);
                }
            }
        };

        testBackupsTemplate(MULTI_MAP_NAME, BACKUP_COUNT, 2, KEY_COUNT, VALUE_COUNT, c);
    }

    public void testBackupsTemplate(String multiMapName, int backupCount, int asyncBackupCount,
                                    int keyCount, int valueCount, Consumer c) {
        int totalBackupCount = backupCount + asyncBackupCount;
        Config config = new Config();
        config.getMultiMapConfig(multiMapName)
                .setBackupCount(backupCount)
                .setAsyncBackupCount(asyncBackupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        MultiMap<Integer, Integer> multiMap = hz.getMultiMap(multiMapName);
        c.accept(multiMap);

        // scale up
        LOGGER.info("Scaling up to " + (totalBackupCount + 1) + " members...");
        for (int bc = 1; bc <= totalBackupCount; bc++) {
            factory.newHazelcastInstance(config);
            waitAllForSafeState(factory.getAllHazelcastInstances());
            assertEquals(bc + 1, factory.getAllHazelcastInstances().iterator().next().getCluster().getMembers().size());

            assertMultiMapBackups(multiMapName, totalBackupCount, asyncBackupCount, keyCount, valueCount);
        }

        // scale down
        LOGGER.info("Scaling down to 1 member...");
        for (int bc = totalBackupCount - 1; bc > 0; bc--) {
            factory.getAllHazelcastInstances().iterator().next().shutdown();
            waitAllForSafeState(factory.getAllHazelcastInstances());
            assertEquals(bc + 1, factory.getAllHazelcastInstances().iterator().next().getCluster().getMembers().size());

            assertMultiMapBackups(multiMapName, totalBackupCount, asyncBackupCount, keyCount, valueCount);
        }
    }

    private void assertMultiMapBackups(String multiMapName, int backupCount, int asyncBackupCount,
                                       int keyCount, int valueCount) {
        int totalBackupCount = backupCount + asyncBackupCount;
        HazelcastInstance[] instances = factory.getAllHazelcastInstances().toArray(new HazelcastInstance[0]);
        LOGGER.info("Testing " + totalBackupCount + " backups on " + instances.length + " members");

        Map<Integer, Collection<Integer>> backupCollection = getBackupMultiMap(instances, multiMapName);
        assertEqualsStringFormat("expected %d items in backupCollection, but found %d", keyCount, backupCollection.size());
        for (int key = 0; key < keyCount; key++) {
            assertTrue("backupCollection should contain key " + key, backupCollection.containsKey(key));

            Collection<Integer> values = backupCollection.get(key);
            assertEquals("backupCollection for " + key + " should have " + valueCount + " values " + values,
                    valueCount, values.size());
            for (int i = 0; i < valueCount; i++) {
                assertTrue("backupCollection for " + key + " should contain value " + (key + i), values.contains(key + i));
            }
        }
    }
}
