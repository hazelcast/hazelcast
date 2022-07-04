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

package com.hazelcast.client.txn;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.util.AbstractLoadBalancer;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.IQueue;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipAdapter;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
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
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTxnTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;
    private HazelcastInstance server;

    @Before
    public void setup() {
        server = hazelcastFactory.newHazelcastInstance();
        final ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setRedoOperation(true);
        //always start the txn on first member
        config.setLoadBalancer(new AbstractLoadBalancer() {
            @Override
            public Member next() {
                Member[] members = getMembers();
                if (members == null || members.length == 0) {
                    return null;
                }
                return members[0];
            }

            @Override
            public Member nextDataMember() {
                Member[] members = getDataMembers();
                if (members == null || members.length == 0) {
                    return null;
                }
                return members[0];
            }

            @Override
            public boolean canGetNextDataMember() {
                return true;
            }
        });
        client = hazelcastFactory.newHazelcastClient(config);
        hazelcastFactory.newHazelcastInstance();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testTxnRollback() throws Exception {
        final String queueName = randomString();
        final TransactionContext context = client.newTransactionContext();
        CountDownLatch txnRollbackLatch = new CountDownLatch(1);
        final CountDownLatch memberRemovedLatch = new CountDownLatch(1);

        client.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemovedLatch.countDown();
            }
        });

        try {
            context.beginTransaction();
            assertNotNull(context.getTxnId());
            final TransactionalQueue queue = context.getQueue(queueName);
            queue.offer(randomString());

            server.shutdown();

            context.commitTransaction();
            fail("commit should throw exception!!!");
        } catch (TransactionException e) {
            context.rollbackTransaction();
            txnRollbackLatch.countDown();
        }

        assertOpenEventually(txnRollbackLatch);
        assertOpenEventually(memberRemovedLatch);

        final IQueue<Object> q = client.getQueue(queueName);
        assertNull(q.poll());
        assertEquals(0, q.size());
    }

    @Test
    public void testTxnRollbackOnServerCrash() throws Exception {
        final String queueName = randomString();
        final TransactionContext context = client.newTransactionContext();
        CountDownLatch txnRollbackLatch = new CountDownLatch(1);
        final CountDownLatch memberRemovedLatch = new CountDownLatch(1);

        context.beginTransaction();

        final TransactionalQueue queue = context.getQueue(queueName);

        queue.offer(randomString());
        client.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemovedLatch.countDown();
            }
        });
        server.getLifecycleService().terminate();

        try {
            context.commitTransaction();
            fail("commit should throw exception !");
        } catch (TransactionException e) {
            context.rollbackTransaction();
            txnRollbackLatch.countDown();
        }

        assertOpenEventually(txnRollbackLatch);
        assertOpenEventually(memberRemovedLatch);

        final IQueue<Object> q = client.getQueue(queueName);
        assertNull(q.poll());
        assertEquals(0, q.size());
    }

    @Test
    public void testRollbackOnTimeout() {
        String name = randomString();
        IQueue<Object> queue = client.getQueue(name);
        queue.offer(randomString());

        TransactionOptions options = new TransactionOptions().setTimeout(3, TimeUnit.SECONDS);
        TransactionContext context = client.newTransactionContext(options);
        context.beginTransaction();
        try {
            try {
                context.getQueue(name).take();
            } catch (InterruptedException e) {
                fail();
            }
            sleepAtLeastSeconds(5);
            context.commitTransaction();
            fail();
        } catch (TransactionException e) {
            context.rollbackTransaction();
        }
        assertEquals("Queue size should be 1", 1, queue.size());
    }
}
