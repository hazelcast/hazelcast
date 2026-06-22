/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.txnlist;

import com.hazelcast.cluster.Member;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionalList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionListTest extends HazelcastTestSupport {

    @Test
    public void testSingleListAtomicity() throws ExecutionException, InterruptedException {
        final int itemCount = 200;
        final HazelcastInstance instance = createHazelcastInstance();
        final String name = randomString();

        Future<Integer> f = spawn(() -> {
            IList<Object> set = instance.getList(name);
            while (!set.remove("item-1")) {
            }
            return set.size();
        });

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();

        TransactionalList<Object> set = context.getList(name);
        for (int i = 0; i < itemCount; i++) {
            set.add("item-" + i);
        }
        context.commitTransaction();

        int size = f.get();
        assertEquals(itemCount - 1, size);
    }

    @Test
    public void testOrder_WhenMultipleConcurrentTransactionRollback() throws InterruptedException {
        final HazelcastInstance instance = createHazelcastInstance();
        final String name = randomString();
        IList<Integer> list = instance.getList(name);
        list.add(1);
        list.add(2);
        list.add(3);

        TransactionContext firstContext = instance.newTransactionContext();
        firstContext.beginTransaction();
        firstContext.getList(name).remove(1);

        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            TransactionContext secondContext = instance.newTransactionContext();
            secondContext.beginTransaction();
            secondContext.getList(name).remove(2);
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            secondContext.rollbackTransaction();
        });
        thread.start();
        firstContext.rollbackTransaction();
        latch.countDown();
        thread.join();

        assertEquals(1, list.get(0).intValue());
        assertEquals(2, list.get(1).intValue());
        assertEquals(3, list.get(2).intValue());

    }

    @Test
    public void testAdd() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        String item = randomString();
        IList<Object> list = instance.getList(name);
        TransactionContext context = instance.newTransactionContext();
        try {
            context.beginTransaction();
            TransactionalList<Object> txnList = context.getList(name);
            assertTrue(txnList.add(item));
            context.commitTransaction();
        } catch (Exception e) {
            fail(e.getMessage());
            context.rollbackTransaction();
        }
        assertEquals(1, list.size());
        assertEquals(item, list.get(0));
    }

    @Test
    public void testRemove() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        String item = randomString();
        IList<Object> list = instance.getList(name);
        list.add(item);
        TransactionContext context = instance.newTransactionContext();
        try {
            context.beginTransaction();
            TransactionalList<Object> txnList = context.getList(name);
            assertTrue(txnList.remove(item));
            context.commitTransaction();
        } catch (Exception e) {
            fail(e.getMessage());
            context.rollbackTransaction();
        }
        assertEquals(0, list.size());
    }

    @Test
    public void testRemove_withNotContainedItem() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        String item = randomString();
        String notContainedItem = randomString();
        IList<Object> list = instance.getList(name);
        list.add(item);
        TransactionContext context = instance.newTransactionContext();
        try {
            context.beginTransaction();
            TransactionalList<Object> txnList = context.getList(name);
            assertFalse(txnList.remove(notContainedItem));
            context.commitTransaction();
        } catch (Exception e) {
            fail(e.getMessage());
            context.rollbackTransaction();
        }
        assertEquals(1, list.size());
    }

    @Test
    public void testMigrationSerializationNotFails_whenTransactionsAreUsed() {
        Config config = new Config();
        config.setProperty("hazelcast.partition.count", "2");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        String listName = randomString();
        TransactionContext tr = instance1.newTransactionContext();
        tr.beginTransaction();
        TransactionalList<Object> list = tr.getList(listName);
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        tr.commitTransaction();
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        Member owner = instance1.getPartitionService().getPartition(listName).getOwner();
        HazelcastInstance aliveInstance;
        if (instance1.getCluster().getLocalMember().equals(owner)) {
            instance1.shutdown();
            aliveInstance = instance2;
        } else {
            instance2.shutdown();
            aliveInstance = instance1;
        }
        IList<Object> l = aliveInstance.getList(listName);

        for (int i = 0; i < 10; i++) {
            assertEquals(i, l.get(i));
        }
    }
    @Test
    public void transactionShouldBeRolledBack_whenInitiatorTerminatesBeforeCommit() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance master = factory.newHazelcastInstance();
        HazelcastInstance instance = factory.newHazelcastInstance();
        warmUpPartitions(instance);

        String name = generateKeyOwnedBy(master);
        IList<Integer> list = master.getList(name);
        list.add(1);

        waitAllForSafeState(master, instance);

        TransactionOptions options =
                new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);

        TransactionContext context = master.newTransactionContext(options);
        context.beginTransaction();
        TransactionalList<Integer> txList = context.getList(name);
        txList.remove(1);

        master.getLifecycleService().terminate();

        final IList<Integer> list2 = instance.getList(name);

        assertTrueEventually(() -> assertEquals(1, list2.size()));

        assertTrueAllTheTime(() -> assertEquals(1, list2.size()), 3);
    }

    // -------------------------------------------------------------------------
    // Tests for issue #26472: TransactionalList.remove() must not affect the
    // backing IList before commitTransaction() is called.
    // All tests below are EXPECTED TO FAIL until the fix is applied.
    // -------------------------------------------------------------------------

    @Test
    public void testRemove_sizeOfBackingListIsNotChangedBeforeCommit() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        String item = randomString();
        IList<Object> list = instance.getList(name);
        list.add(item);

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        context.getList(name).remove(item);

        assertEquals("backing list size must be 1 before commit", 1, list.size());

        context.commitTransaction();
        assertEquals("backing list size must be 0 after commit", 0, list.size());
    }

    @Test
    public void testRemove_sizeOfBackingListIsRestoredAfterRollback() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        String item = randomString();
        IList<Object> list = instance.getList(name);
        list.add(item);

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        context.getList(name).remove(item);
        context.rollbackTransaction();

        assertEquals("backing list size must be 1 after rollback", 1, list.size());
    }

    @Test
    public void testRemove_containsOnBackingListReturnsTrueBeforeCommit() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        String item = randomString();
        IList<Object> list = instance.getList(name);
        list.add(item);

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        context.getList(name).remove(item);

        assertTrue("backing list must still contain item before commit", list.contains(item));

        context.commitTransaction();
        assertFalse("backing list must not contain item after commit", list.contains(item));
    }

    @Test
    public void testRemove_getByIndexOnBackingListReturnsItemBeforeCommit() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        String item = randomString();
        IList<Object> list = instance.getList(name);
        list.add(item);

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        context.getList(name).remove(item);

        assertEquals("backing list must still return item at index 0 before commit", item, list.get(0));

        context.commitTransaction();
        assertEquals("backing list must be empty after commit", 0, list.size());
    }

    @Test
    public void testRemove_indexOfOnBackingListFindsItemBeforeCommit() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        String item = randomString();
        IList<Object> list = instance.getList(name);
        list.add(item);

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        context.getList(name).remove(item);

        assertEquals("backing list must find item at index 0 before commit", 0, list.indexOf(item));

        context.commitTransaction();
        assertEquals("backing list must not find item after commit", -1, list.indexOf(item));
    }

    @Test
    public void testRemove_subListOnBackingListIncludesItemBeforeCommit() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        IList<Object> list = instance.getList(name);
        list.add("A");
        list.add("B");
        list.add("C");

        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        context.getList(name).remove("B");

        assertEquals("backing list must still have 3 items before commit", 3, list.size());
        assertEquals("subList before commit must contain all 3 items", 3, list.subList(0, 3).size());
        assertTrue("subList before commit must contain 'B'", list.subList(0, 3).contains("B"));

        context.commitTransaction();
        assertEquals("backing list must have 2 items after commit", 2, list.size());
        assertFalse("subList after commit must not contain 'B'", list.subList(0, 2).contains("B"));
    }

    @Test
    public void testRemove_twoPhaseTransaction_sizeNotChangedBeforeCommit() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        String item = randomString();
        IList<Object> list = instance.getList(name);
        list.add(item);

        TransactionOptions options = new TransactionOptions()
                .setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);
        TransactionContext context = instance.newTransactionContext(options);
        context.beginTransaction();
        context.getList(name).remove(item);

        assertEquals("backing list size must be 1 before commit (TWO_PHASE)", 1, list.size());

        context.commitTransaction();
        assertEquals("backing list size must be 0 after commit (TWO_PHASE)", 0, list.size());
    }

    // Reproduces the exact scenario from https://github.com/hazelcast/hazelcast/issues/26472:
    // add + remove in the same TWO_PHASE transaction must not affect the backing IList before commit.
    @Test
    public void testAddAndRemove_backingListIsNotAffectedBeforeCommit() {
        HazelcastInstance instance = createHazelcastInstance();
        String name = randomString();
        IList<String> list = instance.getList(name);
        list.add("initial");

        TransactionOptions options = new TransactionOptions()
                .setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);
        TransactionContext context = instance.newTransactionContext(options);
        context.beginTransaction();

        TransactionalList<String> transactionalList = context.getList(name);
        transactionalList.add("transactionalAdd");
        transactionalList.remove("initial");

        assertEquals("backing list size must be 1 before commit", 1, list.size());
        assertTrue("backing list must still contain 'initial' before commit", list.contains("initial"));
        assertFalse("backing list must not contain 'transactionalAdd' before commit", list.contains("transactionalAdd"));

        context.commitTransaction();

        assertEquals("backing list size must be 1 after commit", 1, list.size());
        assertFalse("backing list must not contain 'initial' after commit", list.contains("initial"));
        assertTrue("backing list must contain 'transactionalAdd' after commit", list.contains("transactionalAdd"));
    }

}
