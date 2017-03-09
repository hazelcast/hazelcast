/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum.queue;

import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class TransactionalQueueQuorumTest extends AbstractQueueQuorumTest {

    @Parameterized.Parameter(0)
    public TransactionOptions options;

    @Parameterized.Parameters(name = "Executing: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{TransactionOptions.getDefault().setTransactionType(ONE_PHASE)},
                new Object[]{TransactionOptions.getDefault().setTransactionType(TWO_PHASE)}
        );
    }

    @BeforeClass
    public static void initialize() throws Exception {
        initializeFiveMemberCluster(QuorumType.READ_WRITE, 3);
        q4.add("foo");
        addQueueData(q4);
        cluster.splitFiveMembersThreeAndTwo();
    }

    @AfterClass
    public static void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test(expected = TransactionException.class)
    public void testTxPollThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = cluster.h4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalQueue<Object> q = transactionContext.getQueue(QUEUE_NAME);
        q.poll();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxTakeThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        TransactionContext transactionContext = cluster.h4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalQueue<Object> q = transactionContext.getQueue(QUEUE_NAME);
        q.take();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxPeekThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = cluster.h4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalQueue<Object> q = transactionContext.getQueue(QUEUE_NAME);
        q.peek();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxOfferThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = cluster.h4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalQueue<Object> q = transactionContext.getQueue(QUEUE_NAME);
        q.offer("");
        transactionContext.commitTransaction();
    }
}
