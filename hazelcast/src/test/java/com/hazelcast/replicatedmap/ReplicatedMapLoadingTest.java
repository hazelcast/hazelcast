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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(value = {QuickTest.class, ParallelTest.class})
public class ReplicatedMapLoadingTest extends ReplicatedMapBaseTest {

    @Test
    public void testAsyncFillup() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory();
        Config config = new Config();
        String mapName = randomMapName();
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig(mapName);
        replicatedMapConfig.setAsyncFillup(true);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap(mapName);
        fillMap(map1, 0, 1000);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap(mapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1000, map1.size());
                assertEquals(1000, map2.size());
            }
        });
        fillMap(map2, 1000, 2000);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map3 = instance3.getReplicatedMap(mapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2000, map1.size());
                assertEquals(2000, map2.size());
                assertEquals(2000, map3.size());
            }
        });
        fillMap(map3, 2000, 3000);
        HazelcastInstance instance4 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map4 = instance4.getReplicatedMap(mapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3000, map1.size());
                assertEquals(3000, map2.size());
                assertEquals(3000, map3.size());
                assertEquals(3000, map4.size());
            }
        });
    }

    @Test
    public void testSyncFillup() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory();
        String mapName = randomMapName();
        Config config = new Config();
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig(mapName);
        replicatedMapConfig.setAsyncFillup(false);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap(mapName);
        int first = 1000;
        int second = 2000;
        int third = 3000;
        fillMap(map1, 0, first);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap(mapName);
        assertEquals(first, map1.size());
        assertEquals(first, map2.size());
        fillMap(map2, first, second);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map3 = instance3.getReplicatedMap(mapName);
        assertEquals(second, map1.size());
        assertEquals(second, map2.size());
        assertEquals(second, map3.size());
        fillMap(map3, second, third);
        HazelcastInstance instance4 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map4 = instance4.getReplicatedMap(mapName);
        assertEquals(third, map1.size());
        assertEquals(third, map2.size());
        assertEquals(third, map3.size());
        assertEquals(third, map4.size());
    }

    private void fillMap(ReplicatedMap<Integer, Integer> map, int start, int end) {
        for (int i = start; i < end; i++) {
            map.put(i, i);
        }
    }


}
