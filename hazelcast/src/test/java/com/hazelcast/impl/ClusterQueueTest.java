/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.*;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusterQueueTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testOffer() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        for (int i = 0; i < 100; i++) {
            assertTrue(q1.offer("item" + i, 100, TimeUnit.SECONDS));
            assertTrue(q2.offer("item" + i, 100, TimeUnit.SECONDS));
        }
        assertEquals("item0", q1.peek());
        assertEquals("item0", q2.peek());
        for (int i = 0; i < 100; i++) {
            assertEquals("item" + i, q1.poll());
            assertEquals("item" + i, q2.poll());
        }
    }

    @Test
    public void testPollTxn() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(new Config());
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        final IQueue q3 = h3.getQueue("default");
        assertTrue(q2.offer("item"));
        assertEquals(1, h2.getMap("q:default").size());
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                Transaction txn = h3.getTransaction();
                txn.begin();
                assertEquals("item", q3.poll());
                latch.countDown();
            }
        }).start();
        latch.await();
        h3.getLifecycleService().shutdown();
        Thread.sleep(2000);
        assertEquals(1, h2.getMap("q:default").size());
        assertEquals("item", q2.poll());
        final HazelcastInstance h4 = Hazelcast.newHazelcastInstance(new Config());
        final IQueue q4 = h4.getQueue("default");
        final CountDownLatch latch2 = new CountDownLatch(1);
        assertTrue(q2.offer("item2"));
        new Thread(new Runnable() {
            public void run() {
                Transaction txn = h1.getTransaction();
                txn.begin();
                assertEquals("item2", q1.poll());
                latch2.countDown();
            }
        }).start();
        latch2.await();
        h1.getLifecycleService().shutdown();
        Thread.sleep(2000);
        assertEquals("item2", q2.poll());
    }

    @Test
    public void testShutdown() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        for (int i = 0; i < 40;) {
            assertTrue(q1.offer("item" + i++, 100, TimeUnit.SECONDS));
            assertTrue(q2.offer("item" + i++, 100, TimeUnit.SECONDS));
        }
        h1.getLifecycleService().shutdown();
        for (int i = 40; i < 100;) {
            assertTrue(q2.offer("item" + i++, 100, TimeUnit.SECONDS));
        }
        for (int i = 0; i < 100; i++) {
            assertEquals("item" + i, q2.poll());
        }
    }

    @Test
    public void testPollNull() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        for (int i = 0; i < 100; i++) {
            assertNull(q1.poll());
            assertNull(q2.poll());
        }
        assertNull(q1.poll(2, TimeUnit.SECONDS));
        assertNull(q2.poll(2, TimeUnit.SECONDS));
    }

    @Test
    public void testTake() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(3000);
                    for (int i = 0; i < 100; i++) {
                        assertTrue(q1.offer("item"));
                        assertTrue(q2.offer("item"));
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        final ExecutorService es = Executors.newFixedThreadPool(50);
        final CountDownLatch latch = new CountDownLatch(200);
        for (int i = 0; i < 100; i++) {
            es.execute(new Runnable() {
                public void run() {
                    try {
                        assertEquals("item", q1.take());
                        latch.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            es.execute(new Runnable() {
                public void run() {
                    try {
                        assertEquals("item", q2.take());
                        latch.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        es.shutdown();
    }

    @Test
    public void testPollLong() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(3000);
                    for (int i = 0; i < 100; i++) {
                        assertTrue(q1.offer("item"));
                        assertTrue(q2.offer("item"));
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        final ExecutorService es = Executors.newFixedThreadPool(50);
        final CountDownLatch latch = new CountDownLatch(200);
        for (int i = 0; i < 100; i++) {
            es.execute(new Runnable() {
                public void run() {
                    try {
                        assertEquals("item", q1.poll(5, TimeUnit.SECONDS));
                        latch.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            es.execute(new Runnable() {
                public void run() {
                    try {
                        assertEquals("item", q2.poll(5, TimeUnit.SECONDS));
                        latch.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        es.shutdown();
    }

    @Test
    public void testOfferLong() throws Exception {
        Config config = new Config();
        config.getQueueConfig("default").setBackingMapRef("default").setMaxSizePerJVM(100);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        for (int i = 0; i < 100; i++) {
            assertTrue(q1.offer("item" + i, 100, TimeUnit.SECONDS));
            assertTrue(q2.offer("item" + i, 100, TimeUnit.SECONDS));
        }
        assertFalse(q1.offer("item"));
        assertFalse(q2.offer("item"));
        assertFalse(q1.offer("item", 2, TimeUnit.SECONDS));
        assertFalse(q2.offer("item", 2, TimeUnit.SECONDS));
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(3000);
                    for (int i = 0; i < 100; i++) {
                        assertEquals("item" + i, q1.poll(2, TimeUnit.SECONDS));
                        assertEquals("item" + i, q2.poll(2, TimeUnit.SECONDS));
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        final ExecutorService es = Executors.newFixedThreadPool(50);
        final CountDownLatch latch = new CountDownLatch(200);
        for (int i = 0; i < 100; i++) {
            es.execute(new Runnable() {
                public void run() {
                    try {
                        assertTrue(q1.offer("item", 30, TimeUnit.SECONDS));
                        latch.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            es.execute(new Runnable() {
                public void run() {
                    try {
                        assertTrue(q2.offer("item", 30, TimeUnit.SECONDS));
                        latch.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        es.shutdown();
    }

    /**
     * Test case for issue 289.
     * <p/>
     * 1. Create instanceA then instanceB, and then a queue on each (same queue name)
     * 2. put a message on queue from instanceB
     * 3. take message off on instanceA
     * 4. shutdown instanceA, then check if queue is still empty on instanceB
     *
     * @throws Exception
     */
    @Test
    public void testQueueAfterShutdown() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IQueue q1 = h1.getQueue("default");
        IQueue q2 = h2.getQueue("default");
        q2.offer("item");
        assertEquals(1, q1.size());
        assertEquals(1, q2.size());
        assertEquals("item", q1.take());
        assertEquals(0, q1.size());
        assertEquals(0, q2.size());
        h1.shutdown();
        assertEquals(0, q2.size());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testQueueAfterShutdown2() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        IQueue q1 = h1.getQueue("default");
        IQueue q2 = h2.getQueue("default");
        q1.offer("item");
        assertEquals(1, q1.size());
        assertEquals(1, q2.size());
        assertEquals("item", q2.take());
        assertEquals(0, q1.size());
        assertEquals(0, q2.size());
        h2.shutdown();
        assertEquals(0, q1.size());
    }

    @Test
    public void queueEntriesShouldBeConsistentAfterShutdown() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Queue<String> q1 = h1.getQueue("q");
        Queue<String> q2 = h2.getQueue("q");
        for (int i = 0; i < 5; i++) {
            q1.offer("item" + i);
        }
        assertEquals(5, q1.size());
        assertEquals(5, q2.size());
        assertEquals("item0", q2.poll());
        assertEquals("item1", q2.poll());
        assertEquals("item2", q2.poll());
        Thread.sleep(10000);
        assertEquals(2, q1.size());
        assertEquals(2, q2.size());
        h1.shutdown();
        Thread.sleep(5000);
        assertEquals(2, q2.size());
    }

    @Test
    public void queueEntriesShouldBeConsistentAfterShutdown2() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        Queue<String> q1 = h1.getQueue("q");
        Queue<String> q2 = h2.getQueue("q");
        for (int i = 0; i < 5; i++) {
            q1.offer("item" + i);
        }
        assertEquals(5, q1.size());
        assertEquals(5, q2.size());
        assertEquals("item0", q1.poll());
        assertEquals("item1", q1.poll());
        assertEquals("item2", q1.poll());
        Thread.sleep(10000);
        assertEquals(2, q1.size());
        assertEquals(2, q2.size());
        h1.shutdown();
        Thread.sleep(5000);
        assertEquals(2, q2.size());
    }

    /**
     * Test case for issue 323
     */
    @Test
    public void testSuperClientWithQueues() {
        Config configSuperClient = new Config();
        configSuperClient.setLiteMember(true);
        HazelcastInstance hNormal = Hazelcast.newHazelcastInstance(new Config());
        final HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(configSuperClient);
        final Queue qSuper = hSuper.getQueue("default");
        final Queue qNormal = hNormal.getQueue("default");
        for (int i = 0; i < 12000; i++) {
            String item = "item" + i;
            qSuper.offer(item);
            assertEquals(item, qNormal.poll());
        }
        for (int i = 0; i < 5000; i++) {
            String item = "item" + i;
            qNormal.offer(item);
            assertEquals(item, qSuper.poll());
        }
    }

    @Test(timeout = 20000)
    public void storedQueueWithExistingItemsAndTransactionRollback() throws InterruptedException {
        final ConcurrentMap<Long, String> STORE =
                new ConcurrentHashMap<Long, String>();
        STORE.put(1l, "Event1");
        STORE.put(2l, "Event2");
        STORE.put(3l, "Event3");
        STORE.put(4l, "Event4");
        STORE.put(5l, "Event5");
        STORE.put(6l, "Event6");
        final CountDownLatch latch = new CountDownLatch(1);
        Config config = new Config();
        config
                .getMapConfig("queue-map")
                .setMapStoreConfig(new MapStoreConfig()
                        .setWriteDelaySeconds(1)
                        .setImplementation(new MapStore<Long, String>() {
                            public String load(Long key) {
                                return STORE.get(key);
                            }

                            public Map<Long, String> loadAll(Collection<Long> keys) {
                                Map<Long, String> result = new HashMap<Long, String>();
                                for (Long key : keys) {
                                    String value = load(key);
                                    if (value != null) {
                                        result.put(key, value);
                                    }
                                }
                                return result;
                            }

                            public Set<Long> loadAllKeys() {
                                return STORE.keySet();
                            }

                            public void store(Long key, String value) {
                                latch.countDown();
                            }

                            public void storeAll(Map<Long, String> map) {
                                for (Map.Entry<Long, String> entry : map.entrySet()) {
                                    store(entry.getKey(), entry.getValue());
                                }
                            }

                            public void delete(Long key) {
                                STORE.remove(key);
                            }

                            public void deleteAll(Collection<Long> keys) {
                                for (Long key : STORE.keySet()) {
                                    delete(key);
                                }
                            }
                        }));
        config.getQueueConfig("tasks").setBackingMapRef("queue-map");
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        IQueue q = h.getQueue("tasks");
        assertEquals(STORE.size(), q.size());
        Transaction t = h.getTransaction();
        t.begin();
        assertEquals(STORE.get(1l), q.poll());
        assertEquals(STORE.get(2l), q.take());
        t.rollback();
        assertFalse(latch.await(10, TimeUnit.SECONDS));
        assertEquals(6, STORE.size());
        assertEquals(6, q.size());
        for (int i = 1; i < 7; i++) {
            assertEquals("Event" + i, q.poll());
        }
    }
}
