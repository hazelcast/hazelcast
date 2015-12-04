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

package com.hazelcast.internal.ascii;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * User: sancar
 * Date: 3/11/13
 * Time: 3:33 PM
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class RestTest extends HazelcastTestSupport{

    final static Config config = new XmlConfigBuilder().build();

    @Before
    public void setup() throws IOException {
        config.setProperty(GroupProperty.REST_ENABLED.getName(),"true");
    }

    @After
    public void tearDown() throws IOException {
        Hazelcast.shutdownAll();
    }
    
    @Test
    public void testTtl_issue1783() throws IOException, InterruptedException {
        String name = "map";

        final CountDownLatch latch = new CountDownLatch(1);
        final MapConfig mapConfig = config.getMapConfig(name);
        mapConfig.setTimeToLiveSeconds(3);
        mapConfig.addEntryListenerConfig(new EntryListenerConfig()
                .setImplementation(new EntryAdapter() {
                    @Override
                    public void entryEvicted(EntryEvent event) {
                        latch.countDown();
                    }
                }));

        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);

        communicator.put(name, "key", "value");
        String value = communicator.get(name, "key");

        assertNotNull(value);
        assertEquals("value", value);

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        value = communicator.get(name, "key");
        assertTrue(value.isEmpty());
    }

    @Test
    public void testRestSimple() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        final String name = "testRestSimple";
        for (int i = 0; i < 100; i++) {
            assertEquals(HttpURLConnection.HTTP_OK, communicator.put(name, String.valueOf(i), String.valueOf(i * 10)));
        }

        for (int i = 0; i < 100; i++) {
            String actual = communicator.get(name, String.valueOf(i));
            assertEquals(String.valueOf(i * 10), actual);
        }

        communicator.deleteAll(name);

        for (int i = 0; i < 100; i++) {
            String actual = communicator.get(name, String.valueOf(i));
            assertEquals("", actual);
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(HttpURLConnection.HTTP_OK, communicator.put(name, String.valueOf(i), String.valueOf(i * 10)));
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(String.valueOf(i * 10), communicator.get(name, String.valueOf(i)));
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(HttpURLConnection.HTTP_OK, communicator.delete(name, String.valueOf(i)));
        }

        for (int i = 0; i < 100; i++) {
            assertEquals("", communicator.get(name, String.valueOf(i)));
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(HttpURLConnection.HTTP_OK, communicator.offer(name, String.valueOf(i)));
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(String.valueOf(i), communicator.poll(name, 2));
        }
    }

    @Test
    public void testQueueSizeEmpty() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        final String name = "testQueueSizeEmpty";

        IQueue queue = instance.getQueue(name);
        Assert.assertEquals(queue.size(), communicator.size(name));
    }

    @Test
    public void testQueueSizeNonEmpty() throws IOException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final HTTPCommunicator communicator = new HTTPCommunicator(instance);
        final String name = "testQueueSizeNotEmpty";
        final int num_items = 100;

        IQueue queue = instance.getQueue(name);

        for (int i = 0; i < num_items; i++) {
            queue.add(i);
        }

        Assert.assertEquals(queue.size(), communicator.size(name));
    }

    @Test
    public void testDisabledRest() throws IOException {
        Config config = new XmlConfigBuilder().build(); //REST should be disabled by default
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String mapName = "testMap";

        try {
            communicator.put("testMap", "1", "1");
        } catch (SocketException ignore) {
        }

        assertEquals(0, instance.getMap(mapName).size());
    }

    @Test
    public void testClusterShutdown() throws IOException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance1);

        assertEquals(HttpURLConnection.HTTP_OK, communicator.shutdownCluster("dev", "dev-pass"));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertFalse(instance1.getLifecycleService().isRunning());
                assertFalse(instance2.getLifecycleService().isRunning());
                assertFalse(instance3.getLifecycleService().isRunning());
            }
        });
    }

    @Test
    public void testGetClusterState() throws IOException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);

        HTTPCommunicator communicator1 = new HTTPCommunicator(instance1);
        HTTPCommunicator communicator2 = new HTTPCommunicator(instance2);

        instance1.getCluster().changeClusterState(ClusterState.FROZEN);
        assertEquals("{\"status\":\"success\",\"state\":\"frozen\"}",
                communicator1.getClusterState("dev", "dev-pass"));

        instance1.getCluster().changeClusterState(ClusterState.PASSIVE);
        assertEquals("{\"status\":\"success\",\"state\":\"passive\"}",
                communicator2.getClusterState("dev", "dev-pass"));

    }

    @Test
    public void testChangeClusterState() throws IOException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance1);

        assertEquals(HttpURLConnection.HTTP_OK, communicator.changeClusterState("dev", "dev-pass", "frozen"));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
            assertEquals(ClusterState.FROZEN, instance1.getCluster().getClusterState());
            assertEquals(ClusterState.FROZEN, instance2.getCluster().getClusterState());
            }
        });
    }
}
