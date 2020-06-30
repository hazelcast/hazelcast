/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.txnqueue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalQueue;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static java.util.Arrays.asList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionQueueBulkOperationsTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private String queueName;

    @Before
    public void setUp() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        instance = factory.newHazelcastInstance();
        queueName = generateKeyOwnedBy(instance);
    }

    @Test
    public void testContainsAll() {
        addItems(1, 2, 3);

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalQueue<Integer> queue = context.getQueue(queueName);
            assertTrue(queue.containsAll(asList(1, 2)));
            context.commitTransaction();
        } catch (TransactionException e) {
            context.rollbackTransaction();
            throw e;
        }
    }

    @Test
    public void anyQueueContainsEmptyCollection() {
        addItems(1, 2, 3);

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalQueue<Integer> queue = context.getQueue(queueName);
            assertTrue(queue.containsAll(emptyList()));
            context.commitTransaction();
        } catch (TransactionException e) {
            context.rollbackTransaction();
            throw e;
        }
    }

    @Test
    public void returnFalse_whenQueueContainsNotAllItems() {
        addItems(1, 3);

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalQueue<Integer> queue = context.getQueue(queueName);
            assertFalse(queue.containsAll(asList(1, 2)));
            context.commitTransaction();
        } catch (TransactionException e) {
            context.rollbackTransaction();
            throw e;
        }
    }

    private void addItems(int... items) {
        TransactionContext context;
        TransactionalQueue<Integer> queue;
        context = instance.newTransactionContext();
        context.beginTransaction();

        queue = context.getQueue(queueName);
        for (int item : items) {
            queue.offer(item);
        }

        context.commitTransaction();
    }
}
