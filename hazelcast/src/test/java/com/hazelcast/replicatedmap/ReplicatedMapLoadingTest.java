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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapLoadingTest extends ReplicatedMapAbstractTest {

    @Test
    public void testAsyncFillUp() {
        Config config = new Config();
        String mapName = randomMapName();
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig(mapName);
        replicatedMapConfig.setAsyncFillup(true);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory();
        fillMapsAndAssertMapSizeEventually(nodeFactory, config, mapName);
    }

    @Test
    public void testSyncFillUp() {
        Config config = new Config();
        String mapName = randomMapName();
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig(mapName);
        replicatedMapConfig.setAsyncFillup(false);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory();
        fillMapsAndAssertMapSizeEventually(nodeFactory, config, mapName);
    }

    private void fillMapsAndAssertMapSizeEventually(TestHazelcastInstanceFactory nodeFactory, Config config, String mapName) {
        final int first = 1000;
        final int second = 2000;
        final int third = 3000;

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap(mapName);

        fillMap(map1, 0, first);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap(mapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertMapSize("map1", first, map1);
                assertMapSize("map2", first, map2);
            }
        });

        fillMap(map2, first, second);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map3 = instance3.getReplicatedMap(mapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertMapSize("map1", second, map1);
                assertMapSize("map2", second, map2);
                assertMapSize("map3", second, map3);
            }
        });

        fillMap(map3, second, third);
        HazelcastInstance instance4 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<Integer, Integer> map4 = instance4.getReplicatedMap(mapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertMapSize("map1", third, map1);
                assertMapSize("map2", third, map2);
                assertMapSize("map3", third, map3);
                assertMapSize("map4", third, map4);
            }
        });
    }

    private void fillMap(ReplicatedMap<Integer, Integer> map, int start, int end) {
        for (int i = start; i < end; i++) {
            map.put(i, i);
        }
    }

    private void assertMapSize(String mapName, int expectedMapSize, ReplicatedMap<Integer, Integer> map) {
        assertEquals(format("%s should contain %d elements", mapName, expectedMapSize), expectedMapSize, map.size());
    }
}
