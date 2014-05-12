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
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author ali 3/11/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TransactionQueueTest extends HazelcastTestSupport {

    @Test(expected = IllegalStateException.class)
    public void nestedTransactionTest() {
        final HazelcastInstance instance = createHazelcastInstanceFactory(1).newHazelcastInstance();
        TransactionContext outerTnx = instance.newTransactionContext();
        outerTnx.beginTransaction();
        final String name = randomString();
        final String item = randomString();
        outerTnx.getQueue(name).offer(item);
        try {
            TransactionContext innerTnx = instance.newTransactionContext();
            innerTnx.beginTransaction();
            innerTnx.getQueue(name).offer(item);
            innerTnx.commitTransaction();
        } finally {
            outerTnx.rollbackTransaction();
        }
    }

    @Test
    public void testTransactionalOfferPoll1() throws Exception {
        Config config = new Config();
        final int insCount = 4;
        final String name = "defQueue";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);


        final TransactionContext context = instances[0].newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalQueue<String> q = context.getQueue(name);
            assertTrue(q.offer("ali"));
            String s = q.poll();
            assertEquals("ali", s);
            context.commitTransaction();
        } catch (TransactionException e) {
            context.rollbackTransaction();
            throw e;
        }
        assertEquals(0, getQueue(instances, name).size());
    }

    @Test
    public void testTransactionalOfferPoll2() throws Exception {
        Config config = new Config();
        final int insCount = 4;
        final String name0 = "defQueue0";
        final String name1 = "defQueue1";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    latch.await(5, TimeUnit.SECONDS);
                    sleep(3000);
                    getQueue(instances, name0).offer("item0");
                } catch (InterruptedException ignored) {
                } catch (HazelcastInstanceNotActiveException ignored) {
                }
            }
        }.start();

        final TransactionContext context = instances[0].newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<String> q0 = context.getQueue(name0);
        TransactionalQueue<String> q1 = context.getQueue(name1);
        String s = null;
        latch.countDown();
        try {
            s = q0.poll(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        assertEquals("item0", s);
        q1.offer(s);
        context.commitTransaction();

        assertEquals(0, getQueue(instances, name0).size());
        assertEquals("item0", getQueue(instances, name1).poll());
    }

    @Test
    public void testQueueWithMap() throws Exception {
        Config config = new Config();
        final int insCount = 4;
        final String queueName = "defQueue";
        final String mapName = "defMap";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        instances[0].getMap(mapName).lock("lock1");

        final TransactionContext context = instances[1].newTransactionContext(new TransactionOptions().setTimeout(5, TimeUnit.SECONDS));
        context.beginTransaction();
        try {
            boolean offered = context.getQueue(queueName).offer("item1");
            assertTrue(offered);
            context.getMap(mapName).put("lock1", "value1");
            fail();
        } catch (TransactionException ex) {
            // expected
            context.rollbackTransaction();
        }
        assertEquals(0, instances[0].getQueue(queueName).size());
        assertNull(instances[0].getMap(mapName).get("lock1"));
    }

    @Test
    public void testRollbackQueue() throws Throwable {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        final HazelcastInstance h1 = factory.newHazelcastInstance();

        final TransactionContext transactionContext = h1.newTransactionContext();

        transactionContext.beginTransaction();

        TransactionalQueue<String> queue = transactionContext.getQueue("testq");

        queue.offer("offered-val");

        transactionContext.rollbackTransaction();

        assertNull(h1.getQueue("testq").poll());

    }

    @Test(expected = TransactionNotActiveException.class)
    public void testTxnQueueOuterTransaction() throws Throwable {
        final HazelcastInstance h1 = createHazelcastInstance();

        final TransactionContext transactionContext = h1.newTransactionContext();
        transactionContext.beginTransaction();
        TransactionalQueue<Object> queue = transactionContext.getQueue("testTxnQueueOuterTransaction");
        queue.offer("item");
        transactionContext.commitTransaction();
        queue.poll();
    }

    @Test
    @Category(ProblematicTest.class)
    public void testIssue859And863() throws Exception {
        final int numberOfMessages = 2000;
        final AtomicInteger count = new AtomicInteger();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        HazelcastInstance instance2 = factory.newHazelcastInstance();

        String inQueueName = "in";
        String outQueueName = "out";

        for (int i = 0; i < numberOfMessages; i++) {
            if (!instance1.getQueue(inQueueName).offer(i)) {
                throw new RuntimeException("initial put did not work");
            }
        }

        class MoveMessage extends Thread {

            private final HazelcastInstance hazelcastInstance;
            private final String inQueueName;
            private final String outQueueName;
            private volatile boolean active = true;

            public MoveMessage(HazelcastInstance hazelcastInstance, String inQueueName, String outQueueName) {
                this.hazelcastInstance = hazelcastInstance;
                this.inQueueName = inQueueName;
                this.outQueueName = outQueueName;
            }

            public void run() {
                while (active && count.get() != numberOfMessages && hazelcastInstance.getLifecycleService().isRunning()) {
                    TransactionContext transactionContext = hazelcastInstance.newTransactionContext();
                    try {
                        transactionContext.beginTransaction();
                    } catch (HazelcastInstanceNotActiveException ignored) {
                        break;
                    }
                    try {
                        TransactionalQueue<Object> queue = transactionContext.getQueue(inQueueName);
                        Object value = queue.poll();
                        if (value != null) {
                            TransactionalQueue<Object> outQueue = transactionContext.getQueue(outQueueName);
                            if (!outQueue.offer(value)) {
                                throw new RuntimeException();
                            }
                        }
                        transactionContext.commitTransaction();
                        if (value != null) {
                            count.incrementAndGet();
                        }
                    } catch (Exception e) {
                        try {
                            transactionContext.rollbackTransaction();
                        } catch (HazelcastInstanceNotActiveException ignored) {
                        }
                    }
                }
            }
        }

        MoveMessage moveMessage1 = new MoveMessage(instance1, inQueueName, outQueueName);
        MoveMessage moveMessage2 = new MoveMessage(instance2, inQueueName, outQueueName);
        moveMessage1.start();
        moveMessage2.start();

        while (count.get() < numberOfMessages / 2) {
            sleepMillis(1);
        }
        instance2.getLifecycleService().terminate();
        moveMessage2.active = false;
        moveMessage2.join(10000);
        moveMessage1.join(10000);

        try {
            assertEquals(numberOfMessages, instance1.getQueue(outQueueName).size());
            assertTrue(instance1.getQueue(inQueueName).isEmpty());
        } finally {
            moveMessage1.active = false;
        }
    }

    @Test
    public void testPeekMethod() throws Exception {
        final Config config = new Config();
        final int insCount = 4;
        final String name = "defQueue";
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        final TransactionContext context = instances[0].newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalQueue<String> q = context.getQueue(name);
            final String response1 = q.peek(10, TimeUnit.SECONDS);
            assertNull(response1);

            assertTrue(q.offer("ali"));

            final String response2 = q.peek();
            assertEquals("ali", response2);
            context.commitTransaction();
        } catch (TransactionException e) {
            context.rollbackTransaction();
            throw e;
        }
        assertEquals(1, getQueue(instances, name).size());
    }

    private IQueue getQueue(HazelcastInstance[] instances, String name) {
        final Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)].getQueue(name);
    }

}
