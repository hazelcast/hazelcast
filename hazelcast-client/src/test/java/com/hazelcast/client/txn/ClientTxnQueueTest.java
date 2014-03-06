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

package com.hazelcast.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author ali 6/7/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnQueueTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        hz.shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTransactionalOfferPoll1() throws Exception {
        final String name = "testTransactionalOfferPoll1";

        final TransactionContext context = hz.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<String> q = context.getQueue(name);
        assertTrue(q.offer("ali"));
        String s = q.poll();
        assertEquals("ali",s);
        context.commitTransaction();
        assertEquals(0, hz.getQueue(name).size());
    }

    @Test
    public void testTransactionalOfferPoll2() throws Exception {
        final String name0 = "defQueue0";
        final String name1 = "defQueue1";
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    latch.await(5, TimeUnit.SECONDS);
                    sleep(3000);
                    hz.getQueue(name0).offer("item0");
                } catch (InterruptedException ignored) {
                }
            }
        }.start();

        final TransactionContext context = hz.newTransactionContext();
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

        assertEquals(0, hz.getQueue(name0).size());
        assertEquals("item0", hz.getQueue(name1).poll());
    }

    @Test
    public void testTransactionalPeek() throws Exception {
        final String name = "defQueue";

        final TransactionContext context = hz.newTransactionContext();
        context.beginTransaction();
        TransactionalQueue<String> q = context.getQueue(name);
        assertTrue(q.offer("ali"));
        String s = q.peek();
        assertEquals("ali",s);
        s = q.peek();
        assertEquals("ali",s);
        context.commitTransaction();
        assertEquals(1, hz.getQueue(name).size());
    }

}
