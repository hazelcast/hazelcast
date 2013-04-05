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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.transaction.*;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @ali 3/11/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class TransactionQueueTest {

    @BeforeClass
    public static void init() {
//        System.setProperty("hazelcast.test.use.network","true");
    }

    @Before
    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTransactionalOfferPoll() throws Exception {
        Config config = new Config();
        final int insCount = 4;
        final String name = "defQueue";
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, insCount);

        boolean b = instances[0].executeTransaction(new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.LOCAL),
                new TransactionalTask<Boolean>() {
                    public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                        TransactionalQueue<String> q = context.getQueue(name);
                        assertTrue(q.offer("ali"));
                        String s = q.poll();
                        assertNull(s);
                        return true;
                    }
                });
        assertTrue(b);
        assertEquals(1, getQueue(instances, name).size());
    }

    @Test
    public void testTransactionalOfferPoll1() throws Exception {
        Config config = new Config();
        final int insCount = 4;
        final String name0 = "defQueue0";
        final String name1 = "defQueue1";
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, insCount);
        new Thread() {
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                getQueue(instances, name0).offer("item0");
            }
        }.start();

        boolean b = instances[0].executeTransaction(new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.LOCAL),
                new TransactionalTask<Boolean>() {
                    public Boolean execute(TransactionalTaskContext context) throws TransactionException {
                        TransactionalQueue<String> q0 = context.getQueue(name0);
                        TransactionalQueue<String> q1 = context.getQueue(name1);
                        String s = null;
                        try {
                            s = q0.poll(6, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            fail(e.getMessage());
                            e.printStackTrace();
                        }
                        assertEquals("item0", s);
                        q1.offer(s);
                        return true;
                    }
                });
        assertTrue(b);
        assertEquals(0, getQueue(instances, name0).size());
        assertEquals("item0", getQueue(instances, name1).poll());
    }

    @Test
    public void testQueueWithMap() throws Exception {
        Config config = new Config();
        final int insCount = 4;
        final String queueName = "defQueue";
        final String mapName = "defMap";
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, insCount);
        instances[0].getMap(mapName).lock("lock1");

        try {
            instances[1].executeTransaction(new TransactionOptions().setTimeout(5, TimeUnit.SECONDS)
                    .setTransactionType(TransactionOptions.TransactionType.LOCAL),
                    new TransactionalTask<Object>() {
                        public Object execute(TransactionalTaskContext context) throws TransactionException {
                            boolean offered = context.getQueue(queueName).offer("item1");
                            assertTrue(offered);
                            context.getMap(mapName).put("lock1", "value1");
                            fail();
                            return null;
                        }
                    });
        } catch (TransactionException ex) {
            ex.printStackTrace();
        }


        assertEquals(0, instances[0].getQueue(queueName).size());
        assertNull(instances[0].getMap(mapName).get("lock1"));
    }

    @Test
    public void testFail() throws Exception {
        Config config = new Config();
        final int insCount = 2;
        final String queueName = "defQueue";
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, insCount);
        final HazelcastInstance ins1 = instances[0];
        final HazelcastInstance ins2 = instances[1];
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                TransactionContext context = ins1.newTransactionContext(new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.LOCAL));
                try {
                    context.beginTransaction();
                    for (int i = 0; i < 100; i++) {
                        context.getQueue(queueName).offer("item" + i);
                        Thread.sleep(50);
                    }
                    context.commitTransaction();
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                    context.rollbackTransaction();
                    latch.countDown();
                }
            }
        }.start();
        ins2.getLifecycleService().shutdown();

        assertTrue(latch.await(20, TimeUnit.SECONDS));

        assertEquals(100, ins1.getQueue(queueName).size());

    }

    private IQueue getQueue(HazelcastInstance[] instances, String name) {
        final Random rnd = new Random(System.currentTimeMillis());
        return instances[rnd.nextInt(instances.length)].getQueue(name);
    }
}
