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

import java.util.Collection;
import java.util.Map;

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
    public void testBackups() {
        Config config = new Config();
        config.getMultiMapConfig(MULTI_MAP_NAME)
                .setBackupCount(BACKUP_COUNT)
                .setAsyncBackupCount(0);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        MultiMap<Integer, Integer> multiMap = hz.getMultiMap(MULTI_MAP_NAME);
        for (int i = 0; i < KEY_COUNT; i++) {
            for (int j = 0; j < VALUE_COUNT; j++) {
                multiMap.put(i, i + j);
            }
        }

        // scale up
        LOGGER.info("Scaling up to " + (BACKUP_COUNT + 1) + " members...");
        for (int backupCount = 1; backupCount <= BACKUP_COUNT; backupCount++) {
            factory.newHazelcastInstance(config);
            waitAllForSafeState(factory.getAllHazelcastInstances());
            assertEquals(backupCount + 1, factory.getAllHazelcastInstances().iterator().next().getCluster().getMembers().size());

            assertMultiMapBackups(backupCount);
        }

        // scale down
        LOGGER.info("Scaling down to 1 member...");
        for (int backupCount = BACKUP_COUNT - 1; backupCount > 0; backupCount--) {
            factory.getAllHazelcastInstances().iterator().next().shutdown();
            waitAllForSafeState(factory.getAllHazelcastInstances());
            assertEquals(backupCount + 1, factory.getAllHazelcastInstances().iterator().next().getCluster().getMembers().size());

            assertMultiMapBackups(backupCount);
        }
    }

    private void assertMultiMapBackups(int backupCount) {
        HazelcastInstance[] instances = factory.getAllHazelcastInstances().toArray(new HazelcastInstance[0]);
        LOGGER.info("Testing " + backupCount + " backups on " + instances.length + " members");

        Map<Integer, Collection<Integer>> backupCollection = getBackupMultiMap(instances, MULTI_MAP_NAME);
        assertEqualsStringFormat("expected %d items in backupCollection, but found %d", KEY_COUNT, backupCollection.size());
        for (int key = 0; key < KEY_COUNT; key++) {
            assertTrue("backupCollection should contain key " + key, backupCollection.containsKey(key));

            Collection<Integer> values = backupCollection.get(key);
            assertEquals("backupCollection for " + key + " should have " + VALUE_COUNT + " values " + values,
                    VALUE_COUNT, values.size());
            for (int i = 0; i < VALUE_COUNT; i++) {
                assertTrue("backupCollection for " + key + " should contain value " + (key + i), values.contains(key + i));
            }
        }
    }
}
