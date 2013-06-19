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
import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
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
 * @ali 6/6/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ClientTxnTest {

    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient(null);
        second = Hazelcast.newHazelcastInstance();
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTxnRollback() throws Exception {

        final TransactionContext context = hz.newTransactionContext();
        CountDownLatch latch = new CountDownLatch(1);
        try {
            context.beginTransaction();
            assertNotNull(context.getTxnId());
            final TransactionalQueue queue = context.getQueue("testTxnRollback");
            queue.offer("item");

            server.getLifecycleService().shutdown();

            context.commitTransaction();
            fail("commit should throw exception!!!");
        } catch (Exception e){
            context.rollbackTransaction();
            latch.countDown();
        }


        assertTrue(latch.await(10, TimeUnit.SECONDS));
//
        final IQueue<Object> q = hz.getQueue("testTxnRollback");
        assertNull(q.poll());
        assertEquals(0, q.size());
    }
}
