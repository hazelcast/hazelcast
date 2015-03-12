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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientProperties;
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
import com.hazelcast.core.IExecutorService;
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.LifecycleEvent.LifecycleState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @ali 7/3/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@Ignore
public class ClientRegressionTest
        extends HazelcastTestSupport {

    @After
    @Before
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testInitialMemberListener() throws InterruptedException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final ClientConfig clientConfig = new ClientConfig();
        final CountDownLatch latch1 = new CountDownLatch(1);
        clientConfig.addListenerConfig(new ListenerConfig().setImplementation(new StaticMemberListener(latch1)));
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        assertTrue("Before starting", latch1.await(5, TimeUnit.SECONDS));

        final CountDownLatch latch2 = new CountDownLatch(1);
        client.getCluster().addMembershipListener(new StaticMemberListener(latch2));

        assertTrue("After starting", latch2.await(5, TimeUnit.SECONDS));
    }

    static class StaticMemberListener implements MembershipListener, InitialMembershipListener {

        final CountDownLatch latch;

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
        final Config config1 = new Config();
        config1.getGroupConfig().setName("foo");
        config1.getNetworkConfig().setPort(5701);
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);
        instance1.getMap("map").put("key", "value");

        final Config config2 = new Config();
        config2.getGroupConfig().setName("bar");
        config2.getNetworkConfig().setPort(5702);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("bar");
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Object, Object> map = client.getMap("map");
        assertNull(map.put("key", "value"));
        assertEquals(1, map.size());
    }

    /**
     * Test for issues #267 and #493
     */
    @Test
    @Ignore
    public void testIssue493() throws Exception {

        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();


        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        final ILock lock = client.getLock("lock");

        for (int k = 0; k < 10; k++) {
            lock.lock();
            try {
                Thread.sleep(100);
            } finally {
                lock.unlock();
            }
        }

        lock.lock();
        hz1.shutdown();
        lock.unlock();
    }

    @Test(timeout = 60000)
    public void testOperationRedo() throws Exception {
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final Thread thread = new Thread() {
            public void run() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                hz1.getLifecycleService().shutdown();
            }
        };

        final IMap map = client.getMap("m");
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
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRedoOperation(true);
        clientConfig.setSmartRouting(false);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final Thread thread = new Thread() {
            public void run() {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                hz1.getLifecycleService().shutdown();
            }
        };

        final IMap map = client.getMap("m");
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
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        hz.getQueue("queue");
        hz.getMap("map");
        hz.getSemaphore("s");
        final HazelcastInstance instance = HazelcastClient.newHazelcastClient();
        final Collection<DistributedObject> distributedObjects = instance.getDistributedObjects();
        assertEquals(3, distributedObjects.size());
    }

    @Test
    public void testMapDestroyIssue764() throws Exception {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        assertNoOfDistributedObject("Initially the server should have %d distributed objects, but had %d", 0, server.getDistributedObjects());
        assertNoOfDistributedObject("Initially the client should have %d distributed objects, but had %d", 0, client.getDistributedObjects());

        IMap map = client.getMap("mapToDestroy");
        assertNoOfDistributedObject("After getMap() the server should have %d distributed objects, but had %d", 1, server.getDistributedObjects());
        assertNoOfDistributedObject("After getMap() the client should have %d distributed objects, but had %d", 1, client.getDistributedObjects());

        map.destroy();
        // Get the distributed objects as fast as possible to catch a race condition more likely
        Collection<DistributedObject> serverDistributedObjects = server.getDistributedObjects();
        Collection<DistributedObject> clientDistributedObjects = client.getDistributedObjects();
        assertNoOfDistributedObject("After destroy() the server should should have %d distributed objects, but had %d", 0, serverDistributedObjects);
        assertNoOfDistributedObject("After destroy() the client should should have %d distributed objects, but had %d", 0, clientDistributedObjects);
    }

    private void assertNoOfDistributedObject(String message, int expected, Collection<DistributedObject> distributedObjects) {
        StringBuilder sb = new StringBuilder(message + "\n");
        for (DistributedObject distributedObject : distributedObjects) {
            sb
                    .append("Name: ").append(distributedObject.getName())
                    .append(", Service: ").append(distributedObject.getServiceName())
                    .append(", PartitionKey: ").append(distributedObject.getPartitionKey())
                    .append("\n");
        }
        assertEqualsStringFormat(sb.toString(), expected, distributedObjects.size());
    }

    /**
     * Client hangs at map.get after shutdown
     */
    @Test
    public void testIssue821() {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final IMap<Object, Object> map = client.getMap("default");

        map.put("key1", "value1");

        instance.shutdown();

        try {
            map.get("key1");
            fail();
        } catch (Exception ignored) {
        }
        assertFalse(instance.getLifecycleService().isRunning());
    }

    @Test
    public void testClientConnectionEvents() throws InterruptedException {
        final LinkedList<LifecycleState> list = new LinkedList<LifecycleState>();
        list.offer(LifecycleState.STARTING);
        list.offer(LifecycleState.STARTED);
        list.offer(LifecycleState.CLIENT_CONNECTED);
        list.offer(LifecycleState.CLIENT_DISCONNECTED);
        list.offer(LifecycleState.CLIENT_CONNECTED);


        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final CountDownLatch latch = new CountDownLatch(list.size());
        LifecycleListener listener = new LifecycleListener() {
            public void stateChanged(LifecycleEvent event) {
                final LifecycleState state = list.poll();
                if (state != null && state.equals(event.getState())) {
                    latch.countDown();
                }
            }
        };
        final ListenerConfig listenerConfig = new ListenerConfig(listener);
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(listenerConfig);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(100);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        Thread.sleep(100);

        instance.shutdown();

        Thread.sleep(800);

        Hazelcast.newHazelcastInstance();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    /**
     * add membership listener
     */
    @Test
    public void testIssue1181() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        Hazelcast.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
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

        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final IMap<Object, Object> map = client.getMap("map");
        final MapInterceptorImpl interceptor = new MapInterceptorImpl();

        final String id = map.addInterceptor(interceptor);
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

    static class MapInterceptorImpl implements MapInterceptor {


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
        final SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(5, new PortableFactory() {
            public Portable create(int classId) {
                return new SamplePortable();
            }
        });
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setSerializationConfig(serializationConfig);

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Integer, SamplePortable> sampleMap = client.getMap(randomString());
        sampleMap.put(1, new SamplePortable(666));
        final SamplePortable samplePortable = sampleMap.get(1);
        assertEquals(666, samplePortable.a);
    }

    @Test
    public void testCredentials() {
        final Config config = new Config();
        config.getGroupConfig().setName("foo").setPassword("bar");
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        final ClientConfig clientConfig = new ClientConfig();
        final ClientSecurityConfig securityConfig = clientConfig.getSecurityConfig();
        securityConfig.setCredentialsClassname(MyCredentials.class.getName());

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

    }

    public static class MyCredentials extends UsernamePasswordCredentials {

        public MyCredentials() {
            super("foo", "bar");
        }
    }

    public void testListenerReconnect() throws InterruptedException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final CountDownLatch latch = new CountDownLatch(2);

        final IMap<Object, Object> m = client.getMap("m");
        final String id = m.addEntryListener(new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent event) {
                latch.countDown();
            }
        }, true);


        m.put("key1", "value1");

        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        instance1.shutdown();

        final Thread thread = new Thread() {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    m.put("key2", "value2");
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
        assertTrue(m.removeEntryListener(id));
        assertFalse(m.removeEntryListener("foo"));
    }

    static class SamplePortable implements Portable {
        public int a;

        public SamplePortable(int a) {
            this.a = a;
        }

        public SamplePortable() {

        }

        public int getFactoryId() {
            return 5;
        }

        public int getClassId() {
            return 6;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("a", a);
        }

        public void readPortable(PortableReader reader) throws IOException {
            a = reader.readInt("a");
        }
    }

    @Test
    public void testNearCache_WhenRegisteredNodeIsDead() {

        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        final String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = new NearCacheConfig();

        nearCacheConfig.setName(mapName);
        nearCacheConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(nearCacheConfig);

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Object, Object> map = client.getMap(mapName);

        map.put("a", "b");
        map.get("a"); //put to nearCache

        instance.shutdown();
        Hazelcast.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(null, map.get("a"));
            }
        });

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
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(smart);

        final int tryCount = 5;

        for (int i = 0; i < tryCount; i++) {
            final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
            final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
            final ILock lock = client.getLock("lock");
            assertTrue(lock.tryLock(1, TimeUnit.MINUTES));
            client.getLifecycleService().terminate(); //with client is dead, lock should be released.
            instance.getLifecycleService().terminate();
        }
    }

    @Test
    public void testDeadlock_WhenDoingOperationFromListeners() {
        Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(new ClientConfig().setExecutorPoolSize(1));

        final int putCount = 1000;

        final CountDownLatch latch = new CountDownLatch(putCount);

        final IMap<Object, Object> map1 = client.getMap(randomMapName());
        final IMap<Object, Object> map2 = client.getMap(randomMapName());

        map1.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                map2.put(1, 1);
                latch.countDown();
            }
        }, false);

        for (int i = 0; i < putCount; i++) {
            map1.put(i, i);
        }

        assertOpenEventually("dadas", latch);
    }


    @Test(expected = ExecutionException.class, timeout = 120000)
    public void testGithubIssue3557()
            throws Exception {

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        UnDeserializable unDeserializable = new UnDeserializable(1);
        IExecutorService executorService = client.getExecutorService("default");
        Issue2509Runnable task = new Issue2509Runnable(unDeserializable);
        Future<?> future = executorService.submitToMember(task, hz.getCluster().getLocalMember());
        future.get();
    }

    public static class Issue2509Runnable
            implements Callable<Integer>, DataSerializable {

        private UnDeserializable unDeserializable;

        public Issue2509Runnable() {
        }

        public Issue2509Runnable(UnDeserializable unDeserializable) {
            this.unDeserializable = unDeserializable;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {

            out.writeObject(unDeserializable);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {

            unDeserializable = in.readObject();
        }

        @Override
        public Integer call() {
            return unDeserializable.foo;
        }
    }

    public static class UnDeserializable
            implements DataSerializable {

        private int foo;

        public UnDeserializable(int foo) {
            this.foo = foo;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {

            out.writeInt(foo);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {

            foo = in.readInt();
        }
    }

    @Test
    public void testNearCache_shutdownClient() {
        final ClientConfig clientConfig = new ClientConfig();
        NearCacheConfig invalidateConfig = new NearCacheConfig();
        final String mapName = randomMapName();
        invalidateConfig.setName(mapName);
        invalidateConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(invalidateConfig);
        Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Integer, Integer> map = client.getMap(mapName);

        map.get(1);
        //test should finish without throwing any exception.
        client.shutdown();
    }

    @Test
    public void testClientReconnect_thenCheckRequestsAreRetriedWithoutException() throws Exception {
        final HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        final CountDownLatch clientStartedDoingRequests = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    clientStartedDoingRequests.await();
                } catch (InterruptedException ignored) {
                }

                hazelcastInstance.shutdown();

                Hazelcast.newHazelcastInstance();

            }
        }).start();


        ClientConfig clientConfig = new ClientConfig();
        //Retry all requests
        clientConfig.getNetworkConfig().setRedoOperation(true);
        //retry to connect to cluster forever(never shutdown the client)
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        //Retry all requests forever(until client is shutdown)
        clientConfig.setProperty(ClientProperties.PROP_INVOCATION_TIMEOUT_SECONDS, String.valueOf(Integer.MAX_VALUE));
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        IMap<Object, Object> map = client.getMap(randomMapName());

        int mapSize = 1000;
        for (int i = 0; i < mapSize; i++) {
            if (i == mapSize / 4) {
                clientStartedDoingRequests.countDown();
            }
            try {
                map.put(i, i);
            } catch (Exception e) {
                assertTrue("Requests should not throw exception with this configuration. Last put key: " + i, false);
            }
        }
    }

    @Test
    public void testClusterShutdown_thenCheckOperationsNotHanging() throws Exception {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        //Retry all requests
        clientConfig.getNetworkConfig().setRedoOperation(true);
        //Retry all requests forever(until client is shutdown)
        clientConfig.setProperty(ClientProperties.PROP_INVOCATION_TIMEOUT_SECONDS, String.valueOf(Integer.MAX_VALUE));
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Object, Object> map = client.getMap(randomMapName());
        final int mapSize = 1000;

        final CountDownLatch clientStartedDoingRequests = new CountDownLatch(1);
        int threadCount = 100;

        final CountDownLatch testFinishedLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < mapSize; i++) {
                            if (i == mapSize / 4) {
                                clientStartedDoingRequests.countDown();
                            }

                            map.put(i, i);

                        }
                    } catch (Throwable ignored) {
                    } finally {
                        testFinishedLatch.countDown();
                    }
                }
            });
            thread.start();
        }

        assertTrue(clientStartedDoingRequests.await(30, TimeUnit.SECONDS));

        hazelcastInstance.shutdown();

        assertOpenEventually("Put operations should not hang.", testFinishedLatch);

    }
}
