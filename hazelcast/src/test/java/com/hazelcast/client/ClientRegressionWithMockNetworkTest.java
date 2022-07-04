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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.client.impl.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.MessageListener;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.core.LifecycleEvent.LifecycleState;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientRegressionWithMockNetworkTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void testOperationRedo() {
        HazelcastInstance hz1 = hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            hz1.getLifecycleService().shutdown();
        });

        IMap<Integer, String> map = client.getMap("m");
        thread.start();
        int expected = 1000;
        for (int i = 0; i < expected; i++) {
            map.put(i, "item" + i);
        }
        assertJoinable(thread);
        assertEquals(expected, map.size());
    }

    @Test
    public void testOperationRedo_smartRoutingDisabled() {
        HazelcastInstance hz1 = hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.getNetworkConfig().setSmartRouting(false);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            hz1.getLifecycleService().shutdown();
        });

        IMap<Integer, Integer> map = client.getMap("m");
        thread.start();
        int expected = 1000;
        for (int i = 0; i < expected; i++) {
            map.put(i, i);
        }
        assertJoinable(thread);
        assertEquals(expected, map.size());
    }

    @Test
    public void testGetDistributedObjectsIssue678() {
        HazelcastInstance hz = hazelcastFactory.newHazelcastInstance();
        hz.getQueue("queue");
        hz.getMap("map");
        hz.getList("list");
        HazelcastInstance instance = hazelcastFactory.newHazelcastClient();
        Collection<DistributedObject> distributedObjects = instance.getDistributedObjects();
        assertEquals(3, distributedObjects.size());
    }

    @Test
    public void testMapDestroyIssue764() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        assertNoOfDistributedObject("Initially the server should have %d distributed objects, but had %d", 0, server.getDistributedObjects());
        assertNoOfDistributedObject("Initially the client should have %d distributed objects, but had %d", 0, client.getDistributedObjects());

        IMap<Object, Object> map = client.getMap("mapToDestroy");
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
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(5000);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> map = client.getMap("default");

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
    public void testClientConnectionEvents() {
        LinkedList<LifecycleState> list = new LinkedList<>();
        list.offer(LifecycleState.STARTING);
        list.offer(LifecycleState.STARTED);
        list.offer(LifecycleState.SHUTTING_DOWN);
        list.offer(LifecycleState.SHUTDOWN);

        //to make sure event CONNECT/DISCONNECT order is correct
        AtomicReference<Object> stateAtomicReference = new AtomicReference<>();
        stateAtomicReference.set(CLIENT_DISCONNECTED);
        Object FAILURE = new Object();

        hazelcastFactory.newHazelcastInstance();
        CountDownLatch latch = new CountDownLatch(list.size());
        CountDownLatch connectedLatch = new CountDownLatch(2);
        CountDownLatch disconnectedLatch = new CountDownLatch(2);
        LifecycleListener listener = new LifecycleListener() {
            public void stateChanged(LifecycleEvent event) {
                Logger.getLogger(getClass()).info("stateChanged: " + event);
                LifecycleState eventState = event.getState();

                if (CLIENT_CONNECTED.equals(eventState)) {
                    if (!stateAtomicReference.compareAndSet(CLIENT_DISCONNECTED, CLIENT_CONNECTED)) {
                        //fail the test
                        stateAtomicReference.set(FAILURE);
                    }
                    connectedLatch.countDown();
                } else if (CLIENT_DISCONNECTED.equals(eventState)) {
                    if (!stateAtomicReference.compareAndSet(CLIENT_CONNECTED, CLIENT_DISCONNECTED)) {
                        //fail the test
                        stateAtomicReference.set(FAILURE);
                    }
                    disconnectedLatch.countDown();
                } else {
                    LifecycleState state = list.poll();
                    if (state != null && state.equals(eventState)) {
                        latch.countDown();
                    }
                }
            }
        };
        ListenerConfig listenerConfig = new ListenerConfig(listener);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(listenerConfig);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance hazelcastClient = hazelcastFactory.newHazelcastClient(clientConfig);

        hazelcastFactory.shutdownAllMembers();

        hazelcastFactory.newHazelcastInstance();

        assertOpenEventually("Expected at least two CLIENT_CONNECTED events!", connectedLatch);

        hazelcastFactory.shutdownAllMembers();

        //wait for disconnect then call client.shutdown(). Otherwise shutdown could prevent firing DISCONNECTED event
        assertOpenEventually("Expected at least two CLIENT_DISCONNECTED events!", disconnectedLatch);

        hazelcastClient.shutdown();

        assertOpenEventually("LifecycleState failed", latch);

        assertNotEquals("CLIENT_CONNECTED CLIENT_DISCONNECTED order was broken ", FAILURE, stateAtomicReference.get());
    }

    @Test
    public void testInterceptor() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

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
        hazelcastFactory.newHazelcastInstance();
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(5, classId -> new SamplePortable());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setSerializationConfig(serializationConfig);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Integer, SamplePortable> sampleMap = client.getMap(randomString());
        sampleMap.put(1, new SamplePortable(666));
        SamplePortable samplePortable = sampleMap.get(1);
        assertEquals(666, samplePortable.a);
    }

    @Test
    public void testCredentials() {
        Config config = new Config();
        config.setClusterName("foo");
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("foo");
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(1000);
        ClientSecurityConfig securityConfig = clientConfig.getSecurityConfig();
        securityConfig.setCredentials(new MyCredentials());
        // not null username/password credentials are not allowed when Security is disabled
        expectedException.expect(IllegalStateException.class);
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    public static class MyCredentials extends UsernamePasswordCredentials {

        public MyCredentials() {
            super("foo", "bar");
        }
    }

    public void testListenerReconnect() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        CountDownLatch latch = new CountDownLatch(2);

        IMap<Object, Object> m = client.getMap("m");
        UUID id = m.addEntryListener(new EntryAdapter<Object, Object>() {
            public void entryAdded(EntryEvent<Object, Object> event) {
                latch.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
                latch.countDown();
            }
        }, true);


        m.put("key1", "value1");

        hazelcastFactory.newHazelcastInstance();
        instance.shutdown();

        Thread thread = new Thread() {
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
        assertJoinable(thread);
        assertTrue(m.removeEntryListener(id));
        assertFalse(m.removeEntryListener(UUID.randomUUID()));
    }

    static class SamplePortable implements Portable {

        public int a;

        SamplePortable(int a) {
            this.a = a;
        }

        SamplePortable() {
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
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = new NearCacheConfig();

        nearCacheConfig.setName(mapName);
        nearCacheConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(nearCacheConfig);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> map = client.getMap(mapName);

        map.put("a", "b");
        // populate Near Cache
        map.get("a");

        instance.shutdown();
        hazelcastFactory.newHazelcastInstance();

        assertTrueEventually(() -> assertNull(map.get("a")));
    }

    @Test
    public void testDeadlock_WhenDoingOperationFromListeners() {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(new ClientConfig());

        int putCount = 1000;

        CountDownLatch latch = new CountDownLatch(putCount);

        IMap<Object, Object> map1 = client.getMap(randomMapName());
        IMap<Object, Object> map2 = client.getMap(randomMapName());

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

        assertOpenEventually(latch);
    }

    @Test
    public void testDeadlock_WhenDoingOperationFromLifecycleListener() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(1);
        IMap<Object, Object> map = client.getMap(randomMapName());

        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == CLIENT_DISCONNECTED) {
                disconnectedLatch.countDown();
                for (int i = 0; i < 1000; i++) {
                    map.get(i);
                }
                finishedLatch.countDown();
            }
        });

        instance.shutdown();
        assertOpenEventually(disconnectedLatch);
        hazelcastFactory.newHazelcastInstance();
        assertOpenEventually(finishedLatch);
    }

    @Test
    public void testDeadlock_WhenDoingOperationFromLifecycleListenerWithInitialPartitionTable() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(1);
        IMap<Object, Object> map = client.getMap(randomMapName());

        // Let the partition table retrieved the first time
        map.get(1);

        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == CLIENT_DISCONNECTED) {
                disconnectedLatch.countDown();
                for (int i = 0; i < 1000; i++) {
                    map.get(i);
                }
                finishedLatch.countDown();
            }
        });

        instance.shutdown();
        assertOpenEventually(disconnectedLatch);
        hazelcastFactory.newHazelcastInstance();
        assertOpenEventually(finishedLatch);
    }

    @Test
    public void testDeadlock_whenDoingOperationFromLifecycleListener_withNearCache() {
        String mapName = randomMapName();

        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(1);

        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setName(mapName)
                .setEvictionConfig(evictionConfig);

        ClientConfig clientConfig = new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(1);
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> map = client.getMap(mapName);

        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == CLIENT_DISCONNECTED) {
                disconnectedLatch.countDown();
                map.get(1);
                map.get(2);
                finishedLatch.countDown();
            }
        });

        instance.shutdown();
        assertOpenEventually(disconnectedLatch);
        hazelcastFactory.newHazelcastInstance();
        assertOpenEventually(finishedLatch);
    }

    @Test(expected = ExecutionException.class, timeout = 120000)
    public void testGithubIssue3557() throws Exception {
        HazelcastInstance hz = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        UnDeserializable unDeserializable = new UnDeserializable(1);
        IExecutorService executorService = client.getExecutorService("default");
        Issue2509Runnable task = new Issue2509Runnable(unDeserializable);
        Future<?> future = executorService.submitToMember(task, hz.getCluster().getLocalMember());
        future.get();
    }

    public static class Issue2509Runnable implements Callable<Integer>, DataSerializable {

        private UnDeserializable unDeserializable;

        public Issue2509Runnable() {
        }

        public Issue2509Runnable(UnDeserializable unDeserializable) {
            this.unDeserializable = unDeserializable;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(unDeserializable);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            unDeserializable = in.readObject();
        }

        @Override
        public Integer call() {
            return unDeserializable.foo;
        }
    }

    public static class UnDeserializable implements DataSerializable {

        private int foo;

        public UnDeserializable(int foo) {
            this.foo = foo;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(foo);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            foo = in.readInt();
        }
    }

    @Test
    public void testNearCache_shutdownClient() {
        ClientConfig clientConfig = new ClientConfig();
        NearCacheConfig invalidateConfig = new NearCacheConfig();
        String mapName = randomMapName();
        invalidateConfig.setName(mapName);
        invalidateConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(invalidateConfig);
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Integer, Integer> map = client.getMap(mapName);

        map.get(1);
        //test should finish without throwing any exception.
        client.shutdown();
    }

    @Test
    public void testClientReconnect_thenCheckRequestsAreRetriedWithoutException() {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        CountDownLatch clientStartedDoingRequests = new CountDownLatch(1);
        new Thread(() -> {
            try {
                clientStartedDoingRequests.await();
            } catch (InterruptedException ignored) {
            }

            hazelcastInstance.shutdown();

            hazelcastFactory.newHazelcastInstance();

        }).start();

        ClientConfig clientConfig = new ClientConfig();
        //Retry all requests
        clientConfig.getNetworkConfig().setRedoOperation(true);
        //retry to connect to cluster forever(never shutdown the client)
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        //Retry all requests forever(until client is shutdown)
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> map = client.getMap(randomMapName());

        int mapSize = 1000;
        for (int i = 0; i < mapSize; i++) {
            if (i == mapSize / 4) {
                clientStartedDoingRequests.countDown();
            }
            try {
                map.put(i, i);
            } catch (Exception e) {
                fail("Requests should not throw exception with this configuration. Last put key: " + i);
            }
        }
    }

    @Test
    public void testClusterShutdown_thenCheckOperationsNotHanging() throws Exception {
        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        //Retry all requests
        clientConfig.getNetworkConfig().setRedoOperation(true);
        //Retry all requests forever(until client is shutdown)
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(5000);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> map = client.getMap(randomMapName());
        int mapSize = 1000;

        CountDownLatch clientStartedDoingRequests = new CountDownLatch(1);
        int threadCount = 100;

        CountDownLatch testFinishedLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(() -> {
                try {
                    for (int i1 = 0; i1 < mapSize; i1++) {
                        if (i1 == mapSize / 4) {
                            clientStartedDoingRequests.countDown();
                        }

                        map.put(i1, i1);

                    }
                } catch (Throwable ignored) {
                } finally {
                    testFinishedLatch.countDown();
                }
            });
            thread.start();
        }

        assertTrue(clientStartedDoingRequests.await(30, TimeUnit.SECONDS));

        hazelcastInstance.shutdown();

        assertOpenEventually("Put operations should not hang.", testFinishedLatch);
    }

    @Test(timeout = 120000)
    public void testMemberAddedWithListeners_thenCheckOperationsNotHanging() {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientExecutionServiceImpl.INTERNAL_EXECUTOR_POOL_SIZE.getName(), "1");
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        IMap<Object, Object> map = client.getMap("map");
        map.addEntryListener(mock(MapListener.class), true);
        HazelcastInstance h2 = hazelcastFactory.newHazelcastInstance();
        String key = generateKeyOwnedBy(h2);
        map.get(key);
    }


    @Test
    @Category(SlowTest.class)
    public void testServerShouldNotCloseClientWhenClientOnlyListening() {
        Config config = new Config();
        int clientHeartbeatSeconds = 8;
        config.setProperty(ClusterProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS.getName(), String.valueOf(clientHeartbeatSeconds));
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "1000");
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastInstance client2 = hazelcastFactory.newHazelcastClient();

        AtomicBoolean isClientDisconnected = new AtomicBoolean();
        client.getLifecycleService().addLifecycleListener(event -> {
            if (CLIENT_DISCONNECTED.equals(event.getState())) {
                isClientDisconnected.set(true);
            }
        });

        String key = "topicName";
        ITopic<Object> topic = client.getTopic(key);
        MessageListener<Object> listener = message -> {
        };
        UUID id = topic.addMessageListener(listener);

        ITopic<Object> client2Topic = client2.getTopic(key);
        long begin = System.currentTimeMillis();

        while (System.currentTimeMillis() - begin < TimeUnit.SECONDS.toMillis(clientHeartbeatSeconds * 2)) {
            client2Topic.publish("message");
        }

        topic.removeMessageListener(id);
        assertFalse(isClientDisconnected.get());
    }

    @Test(timeout = 20000)
    public void testClientShutdown_shouldNotWaitForNextConnectionAttempt() throws InterruptedException {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        ConnectionRetryConfig connectionRetryConfig = clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig();
        connectionRetryConfig.setInitialBackoffMillis(240000);
        connectionRetryConfig.setMaxBackoffMillis(300000);
        connectionRetryConfig.setMultiplier(1);
        connectionRetryConfig.setClusterConnectTimeoutMillis(Integer.MAX_VALUE);


        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        //client will get into retry
        server.shutdown();
        // we expect this shutdown not to wait for 4 minutes (240000).
        // Test will timeout in 20 seconds. Note that global executor shutdown in ClientExecutionServiceImpl timeout is 30 seconds
        client.shutdown();
    }
}
