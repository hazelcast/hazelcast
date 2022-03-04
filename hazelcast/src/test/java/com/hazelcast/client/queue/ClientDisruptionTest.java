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

package com.hazelcast.client.queue;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class ClientDisruptionTest extends HazelcastTestSupport {

    private static final int CLUSTER_SIZE = 3;
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private List<HazelcastInstance> cluster;

    private HazelcastInstance client1;
    private HazelcastInstance client2;

    @Before
    public void setup() {
        cluster = new ArrayList<HazelcastInstance>(CLUSTER_SIZE);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            cluster.add(hazelcastFactory.newHazelcastInstance());
        }
        client1 = hazelcastFactory.newHazelcastClient();
        client2 = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void queueServerOfferClientsPoll_withNodeShutdown() {
        final int initial = 2000;
        final int max = 8000;

        for (int i = 0; i < initial; i++) {
            getNode(1).getQueue("Q1").offer(i);
            getNode(2).getQueue("Q2").offer(i);
        }

        int expectCount = 0;
        for (int i = initial; i < max; i++) {

            if (i == max / 2) {
                shutdownNode(2);
            }
            final int index = i;

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() {
                    assertTrue(getNode(1).getQueue("Q1").offer(index));
                }
            });

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() {
                    assertTrue(getNode(3).getQueue("Q2").offer(index));
                }
            });

            final int expected = expectCount;

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(expected, client1.getQueue("Q1").poll());
                }
            });

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(expected, client2.getQueue("Q2").poll());
                }
            });

            expectCount++;
        }

        for (int i = expectCount; i < max; i++) {
            assertEquals(i, client1.getQueue("Q1").poll());
            assertEquals(i, client2.getQueue("Q2").poll());
        }
    }

    @Test
    public void mapServerPutClientsGet_withNodeShutdown() {
        final int initial = 200;
        final int max = 800;

        for (int i = 0; i < initial; i++) {
            getNode(2).getMap("m").put(i, i);
        }

        int expectCount = 0;
        for (int i = initial; i < max; i++) {

            if (i == max / 2) {
                shutdownNode(1);
            }
            final int index = i;

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() {
                    assertNull(getNode(2).getMap("m").put(index, index));
                }
            });

            final int expected = expectCount;

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(expected, client1.getMap("m").get(expected));
                }
            });

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(expected, client2.getMap("m").get(expected));
                }
            });

            expectCount++;
        }

        for (int i = expectCount; i < max; i++) {
            assertEquals(i, client1.getMap("m").get(i));
        }
    }

    private HazelcastInstance getNode(int index) {
        return cluster.get(index - 1);
    }

    private void shutdownNode(int index) {
        HazelcastInstance node = getNode(index);
        node.getLifecycleService().shutdown();
    }

}
