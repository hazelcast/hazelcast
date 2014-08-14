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
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.util.AbstractLoadBalancer;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnTest extends HazelcastTestSupport {

    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;

    @Before
    public void init() {
        server = Hazelcast.newHazelcastInstance();
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
        });
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
        final String queueName = randomString();
        final TransactionContext context = hz.newTransactionContext();
        CountDownLatch txnRollbackLatch = new CountDownLatch(1);
        final CountDownLatch memberRemovedLatch = new CountDownLatch(1);

        hz.getCluster().addMembershipListener(new MembershipAdapter() {
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
        } catch (Exception e) {
            context.rollbackTransaction();
            txnRollbackLatch.countDown();
        }

        assertOpenEventually(txnRollbackLatch);
        assertOpenEventually(memberRemovedLatch);

        final IQueue<Object> q = hz.getQueue(queueName);
        assertNull(q.poll());
        assertEquals(0, q.size());
    }

    @Test
    public void testTxnRollbackOnServerCrash() throws Exception {
        final String queueName = randomString();
        final TransactionContext context = hz.newTransactionContext();
        CountDownLatch txnRollbackLatch = new CountDownLatch(1);
        final CountDownLatch memberRemovedLatch = new CountDownLatch(1);

        context.beginTransaction();

        final TransactionalQueue queue = context.getQueue(queueName);

        queue.offer(randomString());
        hz.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemovedLatch.countDown();
            }
        });
        server.getLifecycleService().terminate();

        try {
            context.commitTransaction();
            fail("commit should throw exception !");
        } catch (Exception e) {
            context.rollbackTransaction();
            txnRollbackLatch.countDown();
        }

        assertOpenEventually(txnRollbackLatch);
        assertOpenEventually(memberRemovedLatch);

        final IQueue<Object> q = hz.getQueue(queueName);
        assertNull(q.poll());
        assertEquals(0, q.size());
    }

    private class MembershipAdapter implements MembershipListener {
        @Override
        public void memberAdded(MembershipEvent membershipEvent) {

        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {

        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

        }
    }
}
