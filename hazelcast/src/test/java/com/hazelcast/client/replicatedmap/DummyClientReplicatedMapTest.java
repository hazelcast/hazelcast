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

package com.hazelcast.client.replicatedmap;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DummyClientReplicatedMapTest extends HazelcastTestSupport {

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testGet() throws Exception {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig(instance1));
        ReplicatedMap<String, String> map = client.getReplicatedMap(randomMapName());
        String key = generateKeyOwnedBy(instance2);
        int partitionId = instance2.getPartitionService().getPartition(key).getPartitionId();
        setPartitionId(map, partitionId);
        String value = randomString();
        map.put(key, value);

        assertEquals(value, map.get(key));
    }

    @Test
    public void testIsEmpty() throws Exception {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig(instance1));
        final ReplicatedMap<String, String> map = client.getReplicatedMap(randomMapName());

        String key = generateKeyOwnedBy(instance2);
        int partitionId = instance1.getPartitionService().getPartition(key).getPartitionId();
        setPartitionId(map, partitionId);
        String value = randomString();
        map.put(key, value);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(map.isEmpty());
            }
        });
    }

    @Test
    public void testKeySet() throws Exception {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig(instance1));

        final ReplicatedMap<String, String> map = client.getReplicatedMap(randomMapName());
        final String key = generateKeyOwnedBy(instance2);
        final String value = randomString();

        int partitionId = instance1.getPartitionService().getPartition(key).getPartitionId();
        setPartitionId(map, partitionId);

        map.put(key, value);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Collection<String> keySet = map.keySet();
                assertEquals(1, keySet.size());
                assertEquals(key, keySet.iterator().next());
            }
        });
    }

    @Test
    public void testEntrySet() throws Exception {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig(instance1));

        final ReplicatedMap<String, String> map = client.getReplicatedMap(randomMapName());
        final String key = generateKeyOwnedBy(instance2);
        final String value = randomString();

        int partitionId = instance1.getPartitionService().getPartition(key).getPartitionId();
        setPartitionId(map, partitionId);

        map.put(key, value);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Set<Map.Entry<String, String>> entries = map.entrySet();
                assertEquals(1, entries.size());
                Map.Entry<String, String> entry = entries.iterator().next();
                assertEquals(key, entry.getKey());
                assertEquals(value, entry.getValue());
            }
        });
    }

    @Test
    public void testValues() throws Exception {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig(instance1));

        final ReplicatedMap<String, String> map = client.getReplicatedMap(randomMapName());
        final String key = generateKeyOwnedBy(instance2);
        final String value = randomString();

        int partitionId = instance1.getPartitionService().getPartition(key).getPartitionId();
        setPartitionId(map, partitionId);

        map.put(key, value);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Collection<String> values = map.values();
                assertEquals(1, values.size());
                assertEquals(value, values.iterator().next());
            }
        });
    }

    @Test
    public void testContainsKey() throws Exception {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig(instance1));
        ReplicatedMap<String, String> map = client.getReplicatedMap(randomMapName());

        final String key = generateKeyOwnedBy(instance2);
        int partitionId = instance1.getPartitionService().getPartition(key).getPartitionId();
        setPartitionId(map, partitionId);
        String value = randomString();
        map.put(key, value);

        assertTrue(map.containsKey(key));
    }

    @Test
    public void testContainsValue() throws Exception {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig(instance1));
        final ReplicatedMap<String, String> map = client.getReplicatedMap(randomMapName());

        final String key = generateKeyOwnedBy(instance2);
        int partitionId = instance1.getPartitionService().getPartition(key).getPartitionId();
        setPartitionId(map, partitionId);
        final String value = randomString();
        map.put(key, value);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(map.containsValue(value));
            }
        });
    }

    @Test
    public void testSize() throws Exception {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig(instance1));
        final ReplicatedMap<String, String> map = client.getReplicatedMap(randomMapName());

        String key = generateKeyOwnedBy(instance2);
        int partitionId = instance1.getPartitionService().getPartition(key).getPartitionId();
        setPartitionId(map, partitionId);
        String value = randomString();
        map.put(key, value);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, map.size());
            }
        });
    }

    @Test
    public void testClear() throws Exception {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig(instance1));
        final ReplicatedMap<String, String> map = client.getReplicatedMap(randomMapName());

        String key = generateKeyOwnedBy(instance2);
        int partitionId = instance1.getPartitionService().getPartition(key).getPartitionId();
        setPartitionId(map, partitionId);
        String value = randomString();
        map.put(key, value);
        map.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, map.size());
            }
        });
    }

    @Test
    public void testRemove() throws Exception {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig(instance1));
        final ReplicatedMap<String, String> map = client.getReplicatedMap(randomMapName());

        String key = generateKeyOwnedBy(instance2);
        int partitionId = instance1.getPartitionService().getPartition(key).getPartitionId();
        setPartitionId(map, partitionId);
        String value = randomString();
        map.put(key, value);
        map.remove(key);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, map.size());
            }
        });
    }

    @Test
    public void testPutAll() throws Exception {
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig(instance1));
        final ReplicatedMap<String, String> map = client.getReplicatedMap(randomMapName());

        String key = generateKeyOwnedBy(instance2);
        int partitionId = instance1.getPartitionService().getPartition(key).getPartitionId();
        setPartitionId(map, partitionId);
        String value = randomString();
        HashMap<String, String> m = new HashMap<String, String>();
        m.put(key, value);
        map.putAll(m);

        assertEquals(value, map.get(key));
    }

    private ClientConfig getClientConfig(HazelcastInstance instance) {
        Address address = instance.getCluster().getLocalMember().getAddress();
        String addressString = address.getHost() + ":" + address.getPort();
        ClientConfig dummyClientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = new ClientNetworkConfig();
        networkConfig.setSmartRouting(false);
        networkConfig.addAddress(addressString);
        dummyClientConfig.setNetworkConfig(networkConfig);
        return dummyClientConfig;
    }

    private void setPartitionId(ReplicatedMap<String, String> map, int partitionId) throws Exception {
        Class<?> clazz = map.getClass();
        Field targetPartitionId = clazz.getDeclaredField("targetPartitionId");
        targetPartitionId.setAccessible(true);
        targetPartitionId.setInt(map, partitionId);
    }
}
