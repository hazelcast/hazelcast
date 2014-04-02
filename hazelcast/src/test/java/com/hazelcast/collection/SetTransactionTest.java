package com.hazelcast.collection;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SetTransactionTest extends HazelcastTestSupport {

    static final String ELEMENT = "item";

    @Test
    public void testAdd_withinTxn() throws Exception {
        final String setName = randomString();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newInstances()[0];
        final ISet<String> set = instance.getSet(setName);

        final TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.add(ELEMENT));
        assertEquals(1, txnSet.size());
        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testSetSizeAfterAdd_withinTxn() throws Exception {
        final String setName = randomString();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newInstances()[0];
        final ISet<String> set = instance.getSet(setName);

        final TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        final TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.add(ELEMENT);
        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testRemove_withinTxn() throws Exception {
        final String setName = randomString();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newInstances()[0];
        final ISet<String> set = instance.getSet(setName);
        set.add(ELEMENT);

        final TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.remove(ELEMENT));
        assertFalse(txnSet.remove("NOT_THERE"));
        context.commitTransaction();
        assertEquals(0, set.size());

    }

    @Test
    public void testSetSizeAfterRemove_withinTxn() throws Exception {
        final String setName = randomString();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newInstances()[0];
        final ISet<String> set = instance.getSet(setName);
        set.add(ELEMENT);

        final TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        final TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.remove(ELEMENT);
        context.commitTransaction();
        assertEquals(0, set.size());
    }

    @Test
    public void testAddDuplicateElement_withinTxn() throws Exception {
        final String setName = randomString();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newInstances()[0];

        final TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.add(ELEMENT));
        assertFalse(txnSet.add(ELEMENT));
        context.commitTransaction();
        assertEquals(1, instance.getSet(setName).size());
    }

    @Test
    public void testAddExistingElement_withinTxn() throws Exception {
        final String setName = randomString();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newInstances()[0];
        final ISet<String> set = instance.getSet(setName);
        set.add(ELEMENT);

        final TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertFalse(txnSet.add(ELEMENT));
        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testSetSizeAfterAddingDuplicateElement_withinTxn() throws Exception {
        final String setName = randomString();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newInstances()[0];
        final ISet<String> set = instance.getSet(setName);
        set.add(ELEMENT);

        final TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        final TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.add(ELEMENT);
        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testAddRollBack() throws Exception {
        final String setName = randomString();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newInstances()[0];
        final ISet<String> set = instance.getSet(setName);
        set.add(ELEMENT);

        final TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        final TransactionalSet<Object> setTxn = context.getSet(setName);
        setTxn.add("itemWillGetRollBacked");
        context.rollbackTransaction();
        assertEquals(1, set.size());
    }
}
