/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.LifecycleEvent.LifecycleState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * ali 7/3/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientIssueTest extends HazelcastTestSupport {

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testInitialMemberListener() throws InterruptedException {
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        CountDownLatch beforeStartingLatch = new CountDownLatch(1);
        clientConfig.addListenerConfig(new ListenerConfig().setImplementation(new StaticMemberListener(beforeStartingLatch)));
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        assertTrue("Before starting", beforeStartingLatch.await(5, TimeUnit.SECONDS));

        CountDownLatch afterStartingLatch = new CountDownLatch(1);
        client.getCluster().addMembershipListener(new StaticMemberListener(afterStartingLatch));

        assertTrue("After starting", afterStartingLatch.await(5, TimeUnit.SECONDS));
    }

    private static class StaticMemberListener implements MembershipListener, InitialMembershipListener {
        CountDownLatch latch;

        StaticMemberListener(CountDownLatch latch) {
            this.latch = latch;
        }

        public void init(InitialMembershipEvent event) {
            latch.countDown();
        }

        public void memberAdded(MembershipEvent membershipEvent) {
        }

        public void memberRemoved(MembershipEvent membershipEvent) {
        }

        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        }
    }

    @Test
    public void testClientPortConnection() {
        Config config1 = new Config();
        config1.getGroupConfig().setName("foo");
        config1.getNetworkConfig().setPort(5701);
        HazelcastInstance server1 = Hazelcast.newHazelcastInstance(config1);
        server1.getMap("map").put("key", "value");

        Config config2 = new Config();
        config2.getGroupConfig().setName("bar");
        config2.getNetworkConfig().setPort(5702);
        Hazelcast.newHazelcastInstance(config2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("bar");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        IMap<Object, Object> map = client.getMap("map");
        assertNull(map.put("key", "value"));
        assertEquals(1, map.size());
    }

    /**
     * Test for issues #267 and #493
     */
    @Test
    public void testIssue493() throws Exception {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        ILock lock = client.getLock("lock");

        for (int i = 0; i < 10; i++) {
            lock.lock();
            try {
                Thread.sleep(100);
            } finally {
                lock.unlock();
            }
        }

        lock.lock();
        server.shutdown();
        lock.unlock();
    }

    @Test
    @Category(ProblematicTest.class)
    public void testOperationRedo() throws Exception {
        final HazelcastInstance server = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRedoOperation(true);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        Thread thread = new Thread() {
            public void run() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                server.getLifecycleService().terminate();
            }
        };

        IMap<Integer, String> map = client.getMap("mapName");
        thread.start();
        int expected = 1000;
        for (int i = 0; i < expected; i++) {
            map.put(i, "item" + i);
        }
        thread.join();
        assertEquals(expected, map.size());
    }

    @Test
    public void testOperationRedo_smartRoutingDisabled() throws Exception {
        final HazelcastInstance server = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRedoOperation(true);
        clientConfig.setSmartRouting(false);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        Thread thread = new Thread() {
            public void run() {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                server.getLifecycleService().terminate();
            }
        };

        IMap<Integer, Integer> map = client.getMap("m");
        thread.start();
        int expected = 1000;
        for (int i = 0; i < expected; i++) {
            map.put(i, i);
        }
        thread.join();
        assertEquals(expected, map.size());
    }

    @Test
    public void testGetDistributedObjectsIssue678() {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        server.getQueue("queue");
        server.getMap("map");
        server.getSemaphore("s");

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        Collection<DistributedObject> distributedObjects = client.getDistributedObjects();

        assertEquals(3, distributedObjects.size());
    }

    @Test
    public void testMapDestroyIssue764() throws Exception {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        assertEquals(0, client.getDistributedObjects().size());

        IMap map = client.getMap("m");

        assertEquals(1, client.getDistributedObjects().size());
        map.destroy();

        assertEquals(0, server.getDistributedObjects().size());
        assertEquals(0, client.getDistributedObjects().size());
    }

    /**
     * Client hangs at map.get after shutdown
     */
    @Test
    public void testIssue821() {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IMap<Object, Object> map = client.getMap("default");

        map.put("key1", "value1");

        server.shutdown();

        try {
            map.get("key1");
            fail();
        } catch (Exception ignored) {
        }
        assertFalse(server.getLifecycleService().isRunning());
    }

    @Test
    public void testClientConnectionEvents() throws InterruptedException {
        final LinkedList<LifecycleState> list = new LinkedList<LifecycleState>();
        list.offer(LifecycleState.STARTING);
        list.offer(LifecycleState.STARTED);
        list.offer(LifecycleState.CLIENT_CONNECTED);
        list.offer(LifecycleState.CLIENT_DISCONNECTED);
        list.offer(LifecycleState.CLIENT_CONNECTED);
        final CountDownLatch latch = new CountDownLatch(list.size());

        HazelcastInstance server = Hazelcast.newHazelcastInstance();

        LifecycleListener listener = new LifecycleListener() {
            public void stateChanged(LifecycleEvent event) {
                LifecycleState state = list.poll();
                if (state != null && state.equals(event.getState())) {
                    latch.countDown();
                }
            }
        };
        ListenerConfig listenerConfig = new ListenerConfig(listener);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(listenerConfig);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(100);
        HazelcastClient.newHazelcastClient(clientConfig);

        Thread.sleep(100);

        server.shutdown();

        Thread.sleep(800);

        Hazelcast.newHazelcastInstance();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Add membership listener
     */
    @Test
    public void testIssue1181() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig().setImplementation(new InitialMembershipListener() {
            public void init(InitialMembershipEvent event) {
                for (int i = 0; i < event.getMembers().size(); i++) {
                    latch.countDown();
                }
            }

            public void memberAdded(MembershipEvent membershipEvent) {
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
            }

            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            }
        }));
        HazelcastClient.newHazelcastClient(clientConfig);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testInterceptor() throws InterruptedException {
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IMap<Object, Object> map = client.getMap("map");
        MapInterceptorImpl interceptor = new MapInterceptorImpl();

        String id = map.addInterceptor(interceptor);
        assertNotNull(id);

        map.put("key1", "value");
        assertEquals("value", map.get("key1"));
        map.put("key1", "value1");
        assertEquals("getIntercepted", map.get("key1"));

        assertFalse(map.replace("key1", "getIntercepted", "val"));
        assertTrue(map.replace("key1", "value1", "val"));

        assertEquals("val", map.get("key1"));

        map.put("key2", "oldValue");
        assertEquals("oldValue", map.get("key2"));
        map.put("key2", "newValue");
        assertEquals("putIntercepted", map.get("key2"));

        map.put("key3", "value2");
        assertEquals("value2", map.get("key3"));
        assertEquals("removeIntercepted", map.remove("key3"));
    }

    private static class MapInterceptorImpl implements MapInterceptor {
        MapInterceptorImpl() {
        }

        public Object interceptGet(Object value) {
            if ("value1".equals(value)) {
                return "getIntercepted";
            }
            return null;
        }

        public void afterGet(Object value) {
        }

        public Object interceptPut(Object oldValue, Object newValue) {
            if ("oldValue".equals(oldValue) && "newValue".equals(newValue)) {
                return "putIntercepted";
            }
            return null;
        }

        public void afterPut(Object value) {
        }

        public Object interceptRemove(Object removedValue) {
            if ("value2".equals(removedValue)) {
                return "removeIntercepted";
            }
            return null;
        }

        public void afterRemove(Object value) {
        }
    }

    @Test
    public void testClientPortableWithoutRegisteringToNode() {
        Hazelcast.newHazelcastInstance();
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(5, new PortableFactory() {
            public Portable create(int classId) {
                return new SamplePortable();
            }
        });
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setSerializationConfig(serializationConfig);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        IMap<Integer, SamplePortable> sampleMap = client.getMap(randomString());
        sampleMap.put(1, new SamplePortable(666));
        SamplePortable samplePortable = sampleMap.get(1);
        assertEquals(666, samplePortable.getMember());
    }

    @Test
    public void testCredentials() {
        Config config = new Config();
        config.getGroupConfig().setName("foo").setPassword("bar");
        Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientSecurityConfig securityConfig = clientConfig.getSecurityConfig();
        securityConfig.setCredentialsClassname(MyCredentials.class.getName());

        HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static class MyCredentials extends UsernamePasswordCredentials {
        public MyCredentials() {
            super("foo", "bar");
        }
    }

    @Test
    public void testListenerReconnect() throws InterruptedException {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final CountDownLatch latch = new CountDownLatch(2);

        final IMap<Object, Object> map = client.getMap("m");
        String id = map.addEntryListener(new EntryAdapter<Object, Object>() {
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent event) {
                latch.countDown();
            }
        }, true);


        map.put("key1", "value1");

        Hazelcast.newHazelcastInstance();

        server.shutdown();

        Thread thread = new Thread() {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    map.put("key2", "value2");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        };
        thread.start();

        assertOpenEventually(latch, 10);
        thread.interrupt();
        assertTrue(map.removeEntryListener(id));
        assertFalse(map.removeEntryListener("foo"));
    }

    private static class SamplePortable implements Portable {
        private int member;

        public SamplePortable(int member) {
            this.member = member;
        }

        public SamplePortable() {
        }

        public int getMember() {
            return member;
        }

        @Override
        public int getFactoryId() {
            return 5;
        }

        @Override
        public int getClassId() {
            return 6;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("member", member);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            member = reader.readInt("member");
        }
    }

    @Test
    public void testNearCache_WhenRegisteredNodeIsDead() {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = new NearCacheConfig();

        nearCacheConfig.setName(mapName);
        nearCacheConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(nearCacheConfig);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        IMap<Object, Object> map = client.getMap(mapName);

        map.put("a", "b");
        map.get("a"); //put to nearCache

        server.shutdown();
        Hazelcast.newHazelcastInstance();

        assertEquals(null, map.get("a"));
    }

    @Test
    public void testLock_WhenDummyClientAndOwnerNodeDiesTogether() throws InterruptedException {
        testLock_WhenClientAndOwnerNodeDiesTogether(false);
    }

    @Test
    public void testLock_WhenSmartClientAndOwnerNodeDiesTogether() throws InterruptedException {
        testLock_WhenClientAndOwnerNodeDiesTogether(true);
    }

    private void testLock_WhenClientAndOwnerNodeDiesTogether(boolean smart) throws InterruptedException {
        Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(smart);

        int tryCount = 5;

        for (int i = 0; i < tryCount; i++) {
            HazelcastInstance server = Hazelcast.newHazelcastInstance();
            HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

            ILock lock = client.getLock("lock");
            assertTrue(lock.tryLock(1, TimeUnit.MINUTES));
            client.getLifecycleService().terminate(); //with client is dead, lock should be released.
            server.getLifecycleService().terminate();
        }
    }
}
