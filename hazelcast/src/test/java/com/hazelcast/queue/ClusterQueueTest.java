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

package com.hazelcast.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IQueue;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class ClusterQueueTest extends HazelcastTestSupport {

    @Test
    public void testOffer() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance h1 = instances[0];
        HazelcastInstance h2 = instances[1];
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

    /**
     * Test for issue 730. (google)
     */
    @Test
    public void testDeadTaker() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance h1 = instances[0];
        final HazelcastInstance h2 = instances[1];
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        new Thread(new Runnable() {
            public void run() {
                try {
                    q2.take();
                } catch (HazelcastInstanceNotActiveException ignored) {
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(3000);
                    h2.getLifecycleService().kill();
                    shutdownLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        assertTrue(shutdownLatch.await(100, TimeUnit.SECONDS));
        q1.offer("item");
        assertEquals(1, q1.size());
        assertEquals("item", q1.poll());
    }

    @Test
    public void testShutdown() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance h1 = instances[0];
        final HazelcastInstance h2 = instances[1];
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        for (int i = 0; i < 40; ) {
            assertTrue(q1.offer("item" + i++, 100, TimeUnit.SECONDS));
            assertTrue(q2.offer("item" + i++, 100, TimeUnit.SECONDS));
        }
        h1.getLifecycleService().shutdown();
        for (int i = 40; i < 100; ) {
            assertTrue(q2.offer("item" + i++, 100, TimeUnit.SECONDS));
        }
        for (int i = 0; i < 100; i++) {
            assertEquals("item" + i, q2.poll());
        }
    }

    @Test
    public void testPollNull() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance h1 = instances[0];
        final HazelcastInstance h2 = instances[1];
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
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance h1 = instances[0];
        final HazelcastInstance h2 = instances[1];
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        final CountDownLatch offerLatch = new CountDownLatch(2*100);
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(3000);
                    for (int i = 0; i < 100; i++) {
                        if(q1.offer("item")){
                            offerLatch.countDown();
                        }
                        if(q2.offer("item")){
                            offerLatch.countDown();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        assertTrue(offerLatch.await(100, TimeUnit.SECONDS));
        final ExecutorService es = Executors.newFixedThreadPool(50);
        final CountDownLatch latch = new CountDownLatch(200);
        for (int i = 0; i < 100; i++) {
            es.execute(new Runnable() {
                public void run() {
                    try {
                        if("item".equals(q1.take())){
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            es.execute(new Runnable() {
                public void run() {
                    try {
                        if("item".equals(q2.take())){
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        es.shutdown();
    }

    @Test
    public void testPollLong() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance h1 = instances[0];
        final HazelcastInstance h2 = instances[1];
        final IQueue q1 = h1.getQueue("default");
        final IQueue q2 = h2.getQueue("default");
        final CountDownLatch offerLatch = new CountDownLatch(2*100);
        Thread.sleep(1000);
        new Thread(new Runnable() {
            public void run() {
                for (int i = 0; i < 100; i++) {
                    if(q1.offer("item")){
                        offerLatch.countDown();
                    }
                    if(q2.offer("item")){
                        offerLatch.countDown();
                    }
                }
            }
        }).start();
        assertTrue(offerLatch.await(100, TimeUnit.SECONDS));
        final ExecutorService es = Executors.newFixedThreadPool(50);
        final CountDownLatch latch = new CountDownLatch(200);
        Thread.sleep(3000);
        for (int i = 0; i < 100; i++) {
            es.execute(new Runnable() {
                public void run() {
                    try {
                        if("item".equals(q1.poll(5, TimeUnit.SECONDS))){
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            es.execute(new Runnable() {
                public void run() {
                    try {
                        if("item".equals(q2.poll(5, TimeUnit.SECONDS))){
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        es.shutdown();
    }

    @Test
    public void testOfferLong() throws Exception {
        Config config = new Config();
        config.getQueueConfig("default").setMaxSize(200);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance h1 = instances[0];
        final HazelcastInstance h2 = instances[1];
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
        final CountDownLatch pollLatch = new CountDownLatch(200);
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(3000);
                    for (int i = 0; i < 100; i++) {
                        if (("item" + i).equals(q1.poll(2, TimeUnit.SECONDS))){
                            pollLatch.countDown();
                        }
                        if (("item" + i).equals(q2.poll(2, TimeUnit.SECONDS))){
                            pollLatch.countDown();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        assertTrue(pollLatch.await(100, TimeUnit.SECONDS));
        final ExecutorService es = Executors.newFixedThreadPool(50);
        final CountDownLatch latch = new CountDownLatch(200);
        for (int i = 0; i < 100; i++) {
            es.execute(new Runnable() {
                public void run() {
                    try {
                        if(q1.offer("item", 30, TimeUnit.SECONDS)){
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            es.execute(new Runnable() {
                public void run() {
                    try {
                        if(q2.offer("item", 30, TimeUnit.SECONDS)){
                            latch.countDown();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        assertTrue(latch.await(100, TimeUnit.SECONDS));
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
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance h1 = instances[0];
        final HazelcastInstance h2 = instances[1];
        IQueue q1 = h1.getQueue("default");
        IQueue q2 = h2.getQueue("default");
        q2.offer("item");
        assertEquals(1, q1.size());
        assertEquals(1, q2.size());
        assertEquals("item", q1.take());
        assertEquals(0, q1.size());
        assertEquals(0, q2.size());
        h1.getLifecycleService().shutdown();
        assertEquals(0, q2.size());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testQueueAfterShutdown2() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance h1 = instances[0];
        final HazelcastInstance h2 = instances[1];
        IQueue q1 = h1.getQueue("default");
        IQueue q2 = h2.getQueue("default");
        q1.offer("item");
        assertEquals(1, q1.size());
        assertEquals(1, q2.size());
        assertEquals("item", q2.take());
        assertEquals(0, q1.size());
        assertEquals(0, q2.size());
        h2.getLifecycleService().shutdown();
        assertEquals(0, q1.size());
    }

    @Test
    public void queueEntriesShouldBeConsistentAfterShutdown() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance h1 = instances[0];
        final HazelcastInstance h2 = instances[1];
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
        h1.getLifecycleService().shutdown();
        Thread.sleep(5000);
        assertEquals(2, q2.size());
    }

    @Test
    public void queueEntriesShouldBeConsistentAfterShutdown2() throws Exception {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final HazelcastInstance h1 = instances[0];
        final HazelcastInstance h2 = instances[1];
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
        h1.getLifecycleService().shutdown();
        Thread.sleep(5000);
        assertEquals(2, q2.size());
    }

}
