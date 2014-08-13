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

package com.hazelcast.client.impl.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnTest {

    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;

    @Before
    public void init(){
        server = Hazelcast.newHazelcastInstance();
        final ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setRedoOperation(true);
        hz = HazelcastClient.newHazelcastClient(config);
        second = Hazelcast.newHazelcastInstance();
    }

    @After
    public void destroy() {
        hz.shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTxnRollback() throws Exception {
        final String queueName = "testTxnRollback";
        final TransactionContext context = hz.newTransactionContext();
        CountDownLatch latch = new CountDownLatch(1);
        try {
            context.beginTransaction();
            assertNotNull(context.getTxnId());
            final TransactionalQueue queue = context.getQueue(queueName);
            queue.offer("item");

            server.shutdown();

            context.commitTransaction();
            fail("commit should throw exception!!!");
        } catch (Exception e){
            context.rollbackTransaction();
            latch.countDown();
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        final IQueue<Object> q = hz.getQueue(queueName);
        assertNull(q.poll());
        assertEquals(0, q.size());
    }

    @Test
    @Category(ProblematicTest.class)
    public void testTxnRollbackOnServerCrash() throws Exception {
        final String queueName = "testTxnRollbackOnServerCrash";
        final TransactionContext context = hz.newTransactionContext();
        CountDownLatch latch = new CountDownLatch(1);

        context.beginTransaction();

        final TransactionalQueue queue = context.getQueue(queueName);

        String key = HazelcastTestSupport.generateKeyOwnedBy(server);
        queue.offer(key);
        server.getLifecycleService().terminate();

        try{
            context.commitTransaction();
            fail("commit should throw exception !");
        } catch (Exception e){
            context.rollbackTransaction();
            latch.countDown();
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        final IQueue<Object> q = hz.getQueue(queueName);
        assertNull(q.poll());
        assertEquals(0, q.size());
    }
}
