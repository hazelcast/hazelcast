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

import com.hazelcast.config.Config;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
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
import java.util.concurrent.TimeUnit;

import static com.hazelcast.quorum.QuorumType.READ;
import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class})
public class TransactionalQueueQuorumReadTest extends AbstractQueueQuorumTest {

    @Parameterized.Parameter(0)
    public static TransactionOptions options;

    @Parameterized.Parameter(1)
    public static QuorumType quorumType;

    @Parameterized.Parameters(name = "Executing: {0} {1}")
    public static Collection<Object[]> parameters() {

        TransactionOptions onePhaseOption = TransactionOptions.getDefault();
        onePhaseOption.setTransactionType(ONE_PHASE);

        TransactionOptions twoPhaseOption = TransactionOptions.getDefault();
        twoPhaseOption.setTransactionType(TWO_PHASE);

        return Arrays.asList(
                new Object[]{onePhaseOption, READ},
                new Object[]{twoPhaseOption, READ},
                new Object[]{onePhaseOption, READ_WRITE},
                new Object[]{twoPhaseOption, READ_WRITE}
        );
    }

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void peek_quorum() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalQueue<Object> q = transactionContext.getQueue(QUEUE_NAME + quorumType.name());
        q.peek();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void peek_noQuorum() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalQueue<Object> q = transactionContext.getQueue(QUEUE_NAME + quorumType.name());
        q.peek();
        transactionContext.commitTransaction();
    }

    @Test
    public void peekTimeout_quorum() throws InterruptedException {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalQueue<Object> q = transactionContext.getQueue(QUEUE_NAME + quorumType.name());
        q.peek(10l, TimeUnit.MILLISECONDS);
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void peekTimeout_noQuorum() throws InterruptedException {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalQueue<Object> q = transactionContext.getQueue(QUEUE_NAME + quorumType.name());
        q.peek(10l, TimeUnit.MILLISECONDS);
        transactionContext.commitTransaction();
    }

    @Test
    public void size_quorum() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalQueue<Object> q = transactionContext.getQueue(QUEUE_NAME + quorumType.name());
        q.size();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void size_noQuorum() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalQueue<Object> q = transactionContext.getQueue(QUEUE_NAME + quorumType.name());
        q.size();
        transactionContext.commitTransaction();
    }

    public TransactionContext newTransactionContext(int index) {
        return cluster.instance[index].newTransactionContext(options);
    }
}
