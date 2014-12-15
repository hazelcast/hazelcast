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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Tests the behaviors when {@link com.hazelcast.config.MapConfig#readBackupData} is enabled.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapReadBackupDataTest extends HazelcastTestSupport {

    @Test
    public void testGetOperationCount_whenReadBackupDataEnabled_onSingleNode() throws Exception {
        final IMap<Integer, Integer> map = createReadBackupDataEnabledMap(1);

        map.put(1, 1);
        map.get(1);

        final LocalMapStats stats = map.getLocalMapStats();
        final long getOperationCount = stats.getGetOperationCount();

        assertEquals(1, getOperationCount);

    }

    @Test
    public void testGetOperationCount_whenReadBackupDataEnabled_onMultipleNodes() throws Exception {
        //0. Do initials.
        final String mapName = randomMapName();
        final Config config = getReadBackupDataEnabledConfig(mapName);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] nodes = factory.newInstances(config);
        final HazelcastInstance node1 = nodes[0];
        final HazelcastInstance node2 = nodes[1];

        final IMap<String, Integer> map1 = node1.getMap(mapName);
        final IMap<String, Integer> map2 = node2.getMap(mapName);

        //1. Create keys per each node.
        final String node1Key = generateKeyOwnedBy(node1);
        final String node2Key = generateKeyOwnedBy(node2);

        //2. Populate maps.
        map1.put(node1Key, 0);
        map1.put(node2Key, 1);

        //3. Wait for backups done.
        waitBackupsDone(map1, map2);

        //4. Create get operation count stats.
        map1.get(node1Key);
        map1.get(node2Key);

        final LocalMapStats stats = map1.getLocalMapStats();
        final long getOperationCount = stats.getGetOperationCount();

        //5. Each node should read 1 owned entry and 1 backup entry;
        // only owned entries are counted as get operations. So expected
        // get operation count is 1.
        assertEquals(1, getOperationCount);

    }

    private void waitBackupsDone(final IMap<String, Integer> map1, final IMap<String, Integer> map2) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertBackupDone(map1);
                assertBackupDone(map2);
            }

            private void assertBackupDone(IMap<String, Integer> map1) {
                final LocalMapStats stats1 = map1.getLocalMapStats();
                final long backupEntryCount = stats1.getBackupEntryCount();

                assertEquals(1, backupEntryCount);
            }
        });
    }


    private IMap<Integer, Integer> createReadBackupDataEnabledMap(int nodeCount) {
        final String mapName = randomMapName();
        final Config config = getReadBackupDataEnabledConfig(mapName);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] nodes = factory.newInstances(config);
        final HazelcastInstance node = nodes[0];
        return node.getMap(mapName);
    }

    private Config getReadBackupDataEnabledConfig(String mapName) {
        final Config config = new Config();
        final MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setReadBackupData(true);
        return config;
    }
}
