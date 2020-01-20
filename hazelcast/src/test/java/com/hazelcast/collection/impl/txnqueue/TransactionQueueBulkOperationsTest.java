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
import com.hazelcast.transaction.TransactionalQueue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionQueueBulkOperationsTest extends HazelcastTestSupport {
    @Test
    public void testRetainItemsFromQueueInBulk() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance();
        String name = generateKeyOwnedBy(instance);

        TransactionContext context;
        TransactionalQueue<Integer> queue;

        context = instance.newTransactionContext();
        context.beginTransaction();

        queue = context.getQueue(name);
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);

        context.commitTransaction();

        assertEqualsEventually(() -> spawn(() -> instance.getQueue(name).size()).get(), 3);

        context = instance.newTransactionContext();
        context.beginTransaction();

        queue = context.getQueue(name);

        assertTrue(queue.retainAll(1, 2));

        context.commitTransaction();

        assertEqualsEventually(() -> spawn(() -> instance.getQueue(name).size()).get(), 2);
    }

    @Test
    public void testRetainNoneItemFromQueueInBulk() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance();
        String name = generateKeyOwnedBy(instance);

        TransactionContext context;
        TransactionalQueue<Integer> queue;

        context = instance.newTransactionContext();
        context.beginTransaction();

        queue = context.getQueue(name);
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);

        context.commitTransaction();

        context = instance.newTransactionContext();
        context.beginTransaction();

        queue = context.getQueue(name);

        assertTrue(queue.retainAll(4, 5));

        context.commitTransaction();

        assertEqualsEventually(() -> spawn(() -> instance.getQueue(name).size()).get(), 0);
    }
}
