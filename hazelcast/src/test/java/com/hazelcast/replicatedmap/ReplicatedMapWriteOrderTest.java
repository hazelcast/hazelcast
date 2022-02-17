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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapWriteOrderTest extends ReplicatedMapAbstractTest {

    @Parameters(name = "nodeCount:{0}, operations:{1}, keyCount:{2}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {2, 50, 1}, {2, 50, 10}, {2, 50, 50},
//                {3, 50, 1}, {3, 50, 10}, {3, 50, 50},
//                {3, 10, 10}, {3, 50, 50}, {3, 100, 100},
//                {3, 150, 150}, {3, 200, 200}, {3, 250, 250},
//                {3, 300, 300}, {3, 500, 500}, {3, 750, 750},
//                {3, 500, 1}, {3, 500, 10}, {3, 500, 100}, {3, 500, 500},
//                {3, 1000, 1}, {3, 1000, 10}, {3, 1000, 100}, {3, 1000, 1000},
//                {3, 2000, 1}, {3, 2000, 10}, {3, 2000, 100}, {3, 2000, 1000},
//                {5, 500, 1}, {5, 500, 10}, {5, 500, 100}, {5, 500, 500},
//                {5, 1000, 1}, {5, 1000, 10}, {5, 1000, 100}, {5, 1000, 1000},
//                {10, 1000, 1}, {10, 1000, 10}, {10, 1000, 100}, {10, 1000, 1000},
//                {15, 2000, 1}, {15, 2000, 10}, {15, 2000, 100}, {15, 2000, 1000},
//                {20, 2000, 1}, {20, 2000, 10}, {20, 2000, 100}, {20, 2000, 1000},
//                {20, 5000, 1}, {20, 5000, 10}, {20, 5000, 1000}, {20, 5000, 3000},
        });
    }

    private int nodeCount;
    private int operations;
    private int keyCount;

    public ReplicatedMapWriteOrderTest(int nodeCount, int operations, int keyCount) {
        this.nodeCount = nodeCount;
        this.operations = operations;
        this.keyCount = keyCount;
    }

    @After
    public void setUp() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testDataIntegrity() {
        System.out.println("nodeCount = " + nodeCount);
        System.out.println("operations = " + operations);
        System.out.println("keyCount = " + keyCount);
        Config config = new Config();
        config.getReplicatedMapConfig("test");
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        String replicatedMapName = "test";
        final List<ReplicatedMap<String, Object>> maps = createMapOnEachInstance(instances, replicatedMapName);
        ArrayList<Integer> keys = generateRandomIntegerList(keyCount);
        Thread[] threads = createThreads(nodeCount, maps, keys, operations);
        for (Thread thread : threads) {
            thread.start();
        }
        assertJoinable(threads);
        for (int i = 0; i < keyCount; i++) {
            final String key = "foo-" + keys.get(i);
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    System.out.println("---------------------");
                    System.out.println("key = " + key);
                    printValues();
                    assertValuesAreEqual();
                }

                private void printValues() throws Exception {
                    for (int j = 0; j < maps.size(); j++) {
                        ReplicatedMap map = maps.get(j);
                        System.out.println("value[" + j + "] = " + map.get(key)
                                + ", store version: " + getStore(map, key).getVersion());
                    }
                }

                private void assertValuesAreEqual() {
                    for (int i = 0; i < maps.size() - 1; i++) {
                        ReplicatedMap map1 = maps.get(i);
                        ReplicatedMap map2 = maps.get(i + 1);
                        Object v1 = map1.get(key);
                        Object v2 = map2.get(key);
                        assertNotNull(v1);
                        assertNotNull(v2);
                        assertEquals(v1, v2);
                    }
                }
            }, 120);
        }
    }

    private Thread[] createThreads(int count, List<ReplicatedMap<String, Object>> maps, ArrayList<Integer> keys, int operations) {
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = createPutOperationThread(maps.get(i), keys, operations);
        }
        return threads;
    }

    private Thread createPutOperationThread(final ReplicatedMap<String, Object> map, final ArrayList<Integer> keys,
                                            final int operations) {
        return new Thread(new Runnable() {
            @Override
            public void run() {
                Random random = new Random();
                int size = keys.size();
                for (int i = 0; i < operations; i++) {
                    int index = i % size;
                    String key = "foo-" + keys.get(index);
                    map.put(key, random.nextLong());
                    boolean containsKey = map.containsKey(key);
                    assert containsKey;
                }
            }
        });
    }
}
