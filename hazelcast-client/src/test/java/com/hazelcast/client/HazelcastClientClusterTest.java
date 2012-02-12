/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.ClientProperties.ClientPropertyName;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.impl.GroupProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import static com.hazelcast.client.TestUtility.destroyClients;
import static com.hazelcast.client.TestUtility.newHazelcastClient;
import static com.hazelcast.impl.TestUtil.OrderKey;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class HazelcastClientClusterTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @After
    @Before
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void after() throws Exception {
        destroyClients();
    }

    @Test
    public void testLockOwnerClientDies() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastClient client1 = newHazelcastClient(h1);
        HazelcastClient client2 = newHazelcastClient(h1);
        final IMap c1 = client1.getMap("default");
        final IMap c2 = client2.getMap("default");
        final IMap m1 = h1.getMap("default");
        c1.lock("key");
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                c2.lock("key");
                latch.countDown();
            }
        }).start();
        Thread.sleep(1000); // make sure c1.lock("key") is called and blocked
        client1.shutdown();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testLockWaitingClientDies() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastClient client1 = newHazelcastClient(h1);
        HazelcastClient client2 = newHazelcastClient(h1);
        final IMap c1 = client1.getMap("default");
        final IMap c2 = client2.getMap("default");
        final IMap m1 = h1.getMap("default");
        m1.lock("key");
        new Thread(new Runnable() {
            public void run() {
                c1.lock("key");
            }
        }).start();
        Thread.sleep(1000); // make sure c1.lock("key") is called and blocked
        client1.shutdown();
        Thread.sleep(1000);
        m1.unlock("key");
        assertTrue(c2.tryLock("key"));
    }

    @Test
    public void testAffinity() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h4 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastClient client = newHazelcastClient(h2);
        final IMap c1 = client.getMap("default");
        final IMap m1 = h4.getMap("default");
        int count = 1000;
        OrderKey[] keys = new OrderKey[count];
        for (int i = 0; i < count; i++) {
            OrderKey key = new OrderKey(i, i % 119);
            keys[i] = key;
            m1.put(key, i);
        }
        for (OrderKey key : keys) {
            Member member1 = h1.getPartitionService().getPartition(key).getOwner();
            Member member2 = h1.getPartitionService().getPartition(key.getPartitionKey()).getOwner();
            assertEquals(member1, member2);
            assertEquals(key.getOrderId(), c1.get(key));
            assertEquals(key.getOrderId(), m1.get(key));
        }
    }

    @Test
    public void testUseBackupDataGet() throws Exception {
        final Config config = new Config();
        final MapConfig mapConfig = new MapConfig();
        mapConfig.setName("q");
        mapConfig.setReadBackupData(true);
        config.setMapConfigs(Collections.singletonMap(mapConfig.getName(), mapConfig));
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        h1.getMap("q").put("q", "Q");
        Thread.sleep(50L);
        HazelcastClient client = newHazelcastClient(h2);
        assertEquals("Q", client.getMap("q").get("q"));
    }

    @Test
    public void testNearCache() throws Exception {
        final Config config = new Config();
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setMaxSize(1000);
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap m1 = h1.getMap("default");
        m1.put("1", "value");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IMap m2 = h2.getMap("default");
        assertEquals("value", m2.get("1"));
        assertEquals("value", m1.get("1"));
        HazelcastClient client1 = newHazelcastClient(h1);
        HazelcastClient client2 = newHazelcastClient(h2);
        for (int i = 0; i < 10; i++) {
            assertEquals("value", client1.getMap("default").get("1"));
            assertEquals("value", client2.getMap("default").get("1"));
        }
    }

    @Test
    public void testMembershipListener() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastClient client = newHazelcastClient(h1);
        final CountDownLatch memberAddLatch = new CountDownLatch(1);
        final CountDownLatch memberRemoveLatch = new CountDownLatch(1);
        client.getCluster().addMembershipListener(new MembershipListener() {
            public void memberAdded(MembershipEvent membershipEvent) {
                memberAddLatch.countDown();
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemoveLatch.countDown();
            }
        });
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        h2.getLifecycleService().shutdown();
        assertTrue(memberAddLatch.await(10, TimeUnit.SECONDS));
        assertTrue(memberRemoveLatch.await(10, TimeUnit.SECONDS));
        client.shutdown();
    }

    @Test(expected = IllegalStateException.class, timeout = 50000L)
    public void testNoClusterOnStart() throws Exception {
        final ClientProperties clientProperties =
                ClientProperties.createBaseClientProperties(GroupConfig.DEFAULT_GROUP_NAME, GroupConfig.DEFAULT_GROUP_PASSWORD);
        clientProperties.setPropertyValue(ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_TIMEOUT, "500");
        HazelcastClient.newHazelcastClient(clientProperties, "localhost:5701");
    }

    @Test(expected = NoMemberAvailableException.class, timeout = 50000L)
    public void testNoClusterAfterStart() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final ClientProperties clientProperties =
                ClientProperties.createBaseClientProperties(GroupConfig.DEFAULT_GROUP_NAME, GroupConfig.DEFAULT_GROUP_PASSWORD);
        clientProperties.setPropertyValue(ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_TIMEOUT, "100");
        HazelcastClient client = newHazelcastClient(clientProperties, h1);
        final IMap<Object, Object> map = client.getMap("default");
        map.put("smth", "nothing");
        h1.getLifecycleService().shutdown();
        map.put("smth", "nothing");
    }

    @Test(timeout = 120000L)
    public void testRestartCluster() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final ClientProperties clientProperties =
                ClientProperties.createBaseClientProperties(GroupConfig.DEFAULT_GROUP_NAME, GroupConfig.DEFAULT_GROUP_PASSWORD);
        clientProperties.setPropertyValue(ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_TIMEOUT, "500");
        HazelcastClient client = newHazelcastClient(clientProperties, h1);
        final IMap<String, String> map = client.getMap("default");
        final List<String> values = new CopyOnWriteArrayList<String>();
        map.addEntryListener(new EntryAdapter<String, String>() {
            @Override
            public void entryAdded(EntryEvent<String, String> event) {
                values.add(event.getValue());
            }

            @Override
            public void entryUpdated(EntryEvent<String, String> event) {
                values.add(event.getValue());
            }
        }, true);
        final BlockingQueue<LifecycleState> states = new LinkedBlockingQueue<LifecycleState>();
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {

            public void stateChanged(LifecycleEvent event) {
                states.add(event.getState());
            }
        });
        map.put("smth", "nothing1");
        Thread.sleep(50L);
        assertArrayEquals(values.toString(), new String[]{"nothing1"}, values.toArray(new String[0]));
        h1.getLifecycleService().shutdown();
        assertEquals(LifecycleState.CLIENT_CONNECTION_LOST, states.poll(500L, TimeUnit.MILLISECONDS));
        Thread.sleep(50L);
        try {
            map.put("smth", "nothing2");
            fail("nothing2");
        } catch (NoMemberAvailableException e) {
        }
        try {
            map.put("smth", "nothing3");
            fail("nothing3");
        } catch (NoMemberAvailableException e) {
        }
        h1 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(LifecycleState.CLIENT_CONNECTION_OPENING, states.poll(500L, TimeUnit.MILLISECONDS));
        assertEquals(LifecycleState.CLIENT_CONNECTION_OPENED, states.poll(30000L, TimeUnit.MILLISECONDS));
        map.put("smth", "nothing4");
        Thread.sleep(50L);
        assertArrayEquals(values.toString(), new String[]{"nothing1", "nothing4"}, values.toArray(new String[0]));
    }

    @Test(timeout = 120000L)
    public void testRestartClusterTwice() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final ClientProperties clientProperties =
                ClientProperties.createBaseClientProperties(GroupConfig.DEFAULT_GROUP_NAME, GroupConfig.DEFAULT_GROUP_PASSWORD);
        clientProperties.setPropertyValue(ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_ATTEMPTS_LIMIT, "5");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_TIMEOUT, "1000");
        HazelcastClient client = newHazelcastClient(clientProperties, h1);
        final IMap<String, String> map = client.getMap("default");
        final List<String> values = new ArrayList<String>();
        map.addEntryListener(new EntryAdapter<String, String>() {
            @Override
            public void entryAdded(EntryEvent<String, String> event) {
                values.add(event.getValue());
            }

            @Override
            public void entryUpdated(EntryEvent<String, String> event) {
                values.add(event.getValue());
            }
        }, true);
        final BlockingQueue<LifecycleState> states = new LinkedBlockingQueue<LifecycleState>();
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {

            public void stateChanged(LifecycleEvent event) {
                states.add(event.getState());
            }
        });
        map.put("smth", "nothing");
        for (int i = 0; i < 2; i++) {
            h1.getLifecycleService().shutdown();
            assertEquals(LifecycleState.CLIENT_CONNECTION_LOST, states.poll(500L, TimeUnit.MILLISECONDS));
            try {
                map.put("smth", "nothing-" + i);
                fail();
            } catch (NoMemberAvailableException e) {
            }
            Thread.sleep(50L);
            try {
                map.put("smth", "nothing_" + i);
                fail();
            } catch (NoMemberAvailableException e) {
            }
            Thread.sleep(50L);
            h1 = Hazelcast.newHazelcastInstance(new Config());
            assertEquals(LifecycleState.CLIENT_CONNECTION_OPENING, states.poll(500L, TimeUnit.MILLISECONDS));
            assertEquals(LifecycleState.CLIENT_CONNECTION_OPENED, states.poll(1000L, TimeUnit.MILLISECONDS));
            map.put("smth", "nothing" + i);
            Thread.sleep(50L);
        }
        assertArrayEquals(values.toString(), new String[]{"nothing", "nothing0", "nothing1"}, values.toArray(new String[0]));
    }

    @Test(expected = NoMemberAvailableException.class)
    public void testNoClusterAfterStartIssue328() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final ClientProperties clientProperties =
                ClientProperties.createBaseClientProperties(GroupConfig.DEFAULT_GROUP_NAME, GroupConfig.DEFAULT_GROUP_PASSWORD);
        clientProperties.setPropertyValue(ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_TIMEOUT, "500");
        HazelcastClient client = newHazelcastClient(clientProperties, h1);
        final IMap<Object, Object> map = client.getMap("default");
        h1.getLifecycleService().shutdown();
        map.put("smth", "nothing");
    }
}
