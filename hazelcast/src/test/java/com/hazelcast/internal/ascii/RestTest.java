/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.internal.management.dto.WanReplicationConfigDTO;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test is intentionally not in the {@link com.hazelcast.test.annotation.ParallelTest} category,
 * since it starts real HazelcastInstances which have REST enabled.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RestTest extends HazelcastTestSupport {

    private static AtomicInteger port = new AtomicInteger(5701);

    private Config config = new Config();

    private HazelcastInstance instance;
    private HazelcastInstance remoteInstance;
    private HTTPCommunicator communicator;

    @Before
    public void setup() {
        config.setProperty(GroupProperty.REST_ENABLED.getName(), "true");

        int firstPort = port.getAndIncrement();
        int secondPort = port.getAndIncrement();

        // we start pairs of HazelcastInstances which form a cluster to have remote invocations for all operations
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig()
                .setEnabled(false);
        join.getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1:" + firstPort)
                .addMember("127.0.0.1:" + secondPort);

        config.getNetworkConfig().setPort(firstPort);
        instance = Hazelcast.newHazelcastInstance(config);

        config.getNetworkConfig().setPort(secondPort);
        remoteInstance = Hazelcast.newHazelcastInstance(config);

        communicator = new HTTPCommunicator(instance);
    }

    @After
    public void tearDown() {
        instance.getLifecycleService().terminate();
        remoteInstance.getLifecycleService().terminate();
    }

    @Test
    public void testMapPutGet() throws Exception {
        String name = randomMapName();

        String key = "key";
        String value = "value";

        assertEquals(HTTP_OK, communicator.mapPut(name, key, value));
        assertEquals(value, communicator.mapGet(name, key));
        assertTrue(instance.getMap(name).containsKey(key));
    }

    @Test
    public void testMapPutDelete() throws Exception {
        String name = randomMapName();

        String key = "key";
        String value = "value";

        assertEquals(HTTP_OK, communicator.mapPut(name, key, value));
        assertEquals(HTTP_OK, communicator.mapDelete(name, key));
        assertFalse(instance.getMap(name).containsKey(key));
    }

    @Test
    public void testMapDeleteAll() throws Exception {
        String name = randomMapName();

        int count = 10;
        for (int i = 0; i < count; i++) {
            assertEquals(HTTP_OK, communicator.mapPut(name, "key" + i, "value"));
        }

        IMap<Object, Object> map = instance.getMap(name);
        assertEquals(10, map.size());

        assertEquals(HTTP_OK, communicator.mapDeleteAll(name));
        assertTrue(map.isEmpty());
    }

    // issue #1783
    @Test
    public void testMapTtl() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        EntryListenerConfig listenerConfig = new EntryListenerConfig().setImplementation(new EntryAdapter() {
            @Override
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        });

        String name = randomMapName();
        config.getMapConfig(name)
                .setTimeToLiveSeconds(3)
                .addEntryListenerConfig(listenerConfig);

        String key = "key";
        communicator.mapPut(name, key, "value");

        assertOpenEventually(latch);
        String value = communicator.mapGet(name, key);
        assertTrue(value.isEmpty());
    }

    @Test
    public void testQueueOfferPoll() throws Exception {
        String name = randomName();

        String item = communicator.queuePoll(name, 1);
        assertTrue(item.isEmpty());

        String value = "value";
        assertEquals(HTTP_OK, communicator.queueOffer(name, value));

        IQueue<Object> queue = instance.getQueue(name);
        assertEquals(1, queue.size());

        assertEquals(value, communicator.queuePoll(name, 10));
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testQueueSize() throws Exception {
        String name = randomName();
        IQueue<Integer> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.add(i);
        }

        assertEquals(queue.size(), communicator.queueSize(name));
    }

    @Test
    public void syncMapOverWAN() throws Exception {
        String result = communicator.syncMapOverWAN("atob", "b", "default");
        assertEquals("{\"status\":\"fail\",\"message\":\"WAN sync for map is not supported.\"}", result);
    }

    @Test
    public void syncAllMapsOverWAN() throws Exception {
        String result = communicator.syncMapsOverWAN("atob", "b");
        assertEquals("{\"status\":\"fail\",\"message\":\"WAN sync is not supported.\"}", result);
    }

    @Test
    public void wanClearQueues() throws Exception {
        String result = communicator.wanClearQueues("atob", "b");
        assertEquals("{\"status\":\"fail\",\"message\":\"Clearing WAN replication queues is not supported.\"}", result);
    }

    @Test
    public void addWanConfig() throws Exception {
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName("test");
        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(wanConfig);
        String result = communicator.addWanConfig(dto.toJson().toString());
        assertEquals("{\"status\":\"fail\",\"message\":\"java.lang.UnsupportedOperationException: Adding new WAN config is not supported.\"}", result);
    }

    @Test
    public void testMap_PutGet_withLargeValue() throws IOException {
        String mapName = randomMapName();
        String key = "key";
        int capacity = 10000;
        StringBuilder value = new StringBuilder(capacity);
        while (value.length() < capacity) {
            value.append(randomString());
        }

        int response = communicator.mapPut(mapName, key, value.toString());
        assertEquals(HTTP_OK, response);

        assertEquals(value.toString(), communicator.mapGet(mapName, key));
    }

    @Test
    public void testMap_PutGet_withLargeKey() throws IOException {
        String mapName = randomMapName();
        int capacity = 5000;
        StringBuilder key = new StringBuilder(capacity);
        while (key.length() < capacity) {
            key.append(randomString());
        }

        String value = "value";
        int response = communicator.mapPut(mapName, key.toString(), value);
        assertEquals(HTTP_OK, response);
        assertEquals(value, communicator.mapGet(mapName, key.toString()));
    }
}
