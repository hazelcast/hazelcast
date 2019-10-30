/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
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

import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.core.LifecycleEvent.LifecycleState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testOperationRedo() {
        final HazelcastInstance hz1 = hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

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

        final IMap<Integer, String> map = client.getMap("m");
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
        final HazelcastInstance hz1 = hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.getNetworkConfig().setSmartRouting(false);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

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

        final IMap<Integer, Integer> map = client.getMap("m");
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
        final HazelcastInstance hz = hazelcastFactory.newHazelcastInstance();
        hz.getQueue("queue");
        hz.getMap("map");
        hz.getList("list");
        final HazelcastInstance instance = hazelcastFactory.newHazelcastClient();
        final Collection<DistributedObject> distributedObjects = instance.getDistributedObjects();
        assertEquals(3, distributedObjects.size());
    }

    @Test
    public void testMapDestroyIssue764() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
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
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

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
    public void testClientConnectionEvents() {
        final LinkedList<LifecycleState> list = new LinkedList<LifecycleState>();
        list.offer(LifecycleState.STARTING);
        list.offer(LifecycleState.STARTED);
        list.offer(LifecycleState.CLIENT_CONNECTED);
        list.offer(LifecycleState.CLIENT_DISCONNECTED);
        list.offer(LifecycleState.CLIENT_CONNECTED);
        list.offer(LifecycleState.CLIENT_DISCONNECTED);
        list.offer(LifecycleState.SHUTTING_DOWN);
        list.offer(LifecycleState.SHUTDOWN);

        hazelcastFactory.newHazelcastInstance();
        final CountDownLatch latch = new CountDownLatch(list.size());
        final CountDownLatch connectedLatch = new CountDownLatch(2);
        final CountDownLatch disconnectedLatch = new CountDownLatch(2);
        LifecycleListener listener = new LifecycleListener() {
            public void stateChanged(LifecycleEvent event) {
                Logger.getLogger(getClass()).info("stateChanged: " + event);
                final LifecycleState state = list.poll();
                LifecycleState eventState = event.getState();
                if (state != null && state.equals(eventState)) {
                    latch.countDown();
                }
                if (LifecycleState.CLIENT_CONNECTED.equals(eventState)) {
                    connectedLatch.countDown();
                }
                if (LifecycleState.CLIENT_DISCONNECTED.equals(eventState)) {
                    disconnectedLatch.countDown();
                }
            }
        };
        final ListenerConfig listenerConfig = new ListenerConfig(listener);
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(listenerConfig);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);
        HazelcastInstance hazelcastClient = hazelcastFactory.newHazelcastClient(clientConfig);

        hazelcastFactory.shutdownAllMembers();

        hazelcastFactory.newHazelcastInstance();

        assertOpenEventually("LifecycleState failed. Expected two CLIENT_CONNECTED events!", connectedLatch);

        hazelcastFactory.shutdownAllMembers();

        //wait for disconnect then call client.shutdown(). Otherwise shutdown could prevent firing DISCONNECTED event
        assertOpenEventually("LifecycleState failed. Expected two CLIENT_DISCONNECTED events!", disconnectedLatch);

        hazelcastClient.shutdown();

        assertOpenEventually("LifecycleState failed", latch);
    }

    @Test
    public void testInterceptor() {
        hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

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
        final SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(5, new PortableFactory() {
            public Portable create(int classId) {
                return new SamplePortable();
            }
        });
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setSerializationConfig(serializationConfig);

        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final IMap<Integer, SamplePortable> sampleMap = client.getMap(randomString());
        sampleMap.put(1, new SamplePortable(666));
        final SamplePortable samplePortable = sampleMap.get(1);
        assertEquals(666, samplePortable.a);
    }

    @Test
    public void testCredentials() {
        Config config = new Config();
        config.setClusterName("foo");
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("foo");
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setMaxBackoffMillis(0);
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
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final CountDownLatch latch = new CountDownLatch(2);

        final IMap<Object, Object> m = client.getMap("m");
        final UUID id = m.addEntryListener(new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent event) {
                latch.countDown();
            }
        }, true);


        m.put("key1", "value1");

        hazelcastFactory.newHazelcastInstance();
        instance.shutdown();

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
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);
        final String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = new NearCacheConfig();

        nearCacheConfig.setName(mapName);
        nearCacheConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(nearCacheConfig);

        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final IMap<Object, Object> map = client.getMap(mapName);

        map.put("a", "b");
        // populate Near Cache
        map.get("a");

        instance.shutdown();
        hazelcastFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(map.get("a"));
            }
        });
    }

    @Test
    public void testDeadlock_WhenDoingOperationFromListeners() {
        hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(new ClientConfig().setExecutorPoolSize(1));

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

        assertOpenEventually(latch);
    }

    @Test
    public void testDeadlock_WhenDoingOperationFromLifecycleListener() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig.setExecutorPoolSize(1));

        hazelcastFactory.newHazelcastInstance();
        final CountDownLatch latch = new CountDownLatch(1);
        final IMap<Object, Object> map = client.getMap(randomMapName());

        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleState.CLIENT_DISCONNECTED) {
                    map.get(1);
                    latch.countDown();
                }
            }
        });

        instance.shutdown();
        assertOpenEventually(latch);
    }

    @Test
    public void testDeadlock_WhenDoingOperationFromLifecycleListenerWithInitialPartitionTable() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig.setExecutorPoolSize(1));

        hazelcastFactory.newHazelcastInstance();

        final CountDownLatch latch = new CountDownLatch(1);
        final IMap<Object, Object> map = client.getMap(randomMapName());

        // Let the partition table retrieved the first time
        map.get(1);

        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleState.CLIENT_DISCONNECTED) {
                    for (int i = 0; i < 1000; i++) {
                        map.get(i);
                    }
                    latch.countDown();
                }
            }
        });

        instance.shutdown();
        assertOpenEventually(latch);
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
                .addNearCacheConfig(nearCacheConfig)
                .setExecutorPoolSize(1);

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        hazelcastFactory.newHazelcastInstance();

        final CountDownLatch latch = new CountDownLatch(1);
        final IMap<Object, Object> map = client.getMap(mapName);

        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleState.CLIENT_DISCONNECTED) {
                    map.get(1);
                    map.get(2);
                    latch.countDown();
                }
            }
        });

        instance.shutdown();
        assertOpenEventually(latch);
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
        final ClientConfig clientConfig = new ClientConfig();
        NearCacheConfig invalidateConfig = new NearCacheConfig();
        final String mapName = randomMapName();
        invalidateConfig.setName(mapName);
        invalidateConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(invalidateConfig);
        hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final IMap<Integer, Integer> map = client.getMap(mapName);

        map.get(1);
        //test should finish without throwing any exception.
        client.shutdown();
    }

    @Test
    public void testClientReconnect_thenCheckRequestsAreRetriedWithoutException() {
        final HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance();

        final CountDownLatch clientStartedDoingRequests = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    clientStartedDoingRequests.await();
                } catch (InterruptedException ignored) {
                }

                hazelcastInstance.shutdown();

                hazelcastFactory.newHazelcastInstance();

            }
        }).start();

        ClientConfig clientConfig = new ClientConfig();
        //Retry all requests
        clientConfig.getNetworkConfig().setRedoOperation(true);
        //retry to connect to cluster forever(never shutdown the client)
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setFailOnMaxBackoff(false);
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
                assertTrue("Requests should not throw exception with this configuration. Last put key: " + i, false);
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
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

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

    @Test(timeout = 120000)
    public void testMemberAddedWithListeners_thenCheckOperationsNotHanging() {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientExecutionServiceImpl.INTERNAL_EXECUTOR_POOL_SIZE.getName(), "1");
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        IMap map = client.getMap("map");
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
        config.setProperty(GroupProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS.getName(), String.valueOf(clientHeartbeatSeconds));
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "1000");
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastInstance client2 = hazelcastFactory.newHazelcastClient();

        final AtomicBoolean isClientDisconnected = new AtomicBoolean();
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleState.CLIENT_DISCONNECTED.equals(event.getState())) {
                    isClientDisconnected.set(true);
                }
            }
        });

        String key = "topicName";
        ITopic topic = client.getTopic(key);
        MessageListener listener = new MessageListener() {
            public void onMessage(Message message) {
            }
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
}
