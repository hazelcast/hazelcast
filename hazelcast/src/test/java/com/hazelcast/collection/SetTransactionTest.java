package com.hazelcast.collection;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SetTransactionTest {

    HazelcastInstance instance1;
    HazelcastInstance instance2;

    @Before
    public void init() {
        TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory(2);
        instance1 = instanceFactory.newHazelcastInstance();
        instance2 = instanceFactory.newHazelcastInstance();
    }

    @Test
    public void testAdd_withinTxn() throws Exception {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = instance1.getSet(setName);

        final TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.add(element));
        assertEquals(1, txnSet.size());
        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testSetSizeAfterAdd_withinTxn() throws Exception {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = instance1.getSet(setName);

        final TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.add(element);

        context.commitTransaction();

        assertEquals(1, set.size());
    }

    @Test
    public void testRemove_withinTxn() throws Exception {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = instance1.getSet(setName);
        set.add(element);

        final TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.remove(element));
        assertFalse(txnSet.remove("NOT_THERE"));
        context.commitTransaction();
        assertEquals(0, set.size());

    }

    @Repeat(10)
    @Test
    public void testSetSizeAfterRemove_withinTxn() throws Exception {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = instance1.getSet(setName);
        set.add(element);

        final TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.remove(element);

        context.commitTransaction();

        assertEquals(0, set.size());
    }

    @Test
    public void testAddDuplicateElement_withinTxn() throws Exception {
        final String element = "item1";
        final String setName = randomString();

        final TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.add(element));
        assertFalse(txnSet.add(element));
        context.commitTransaction();
        assertEquals(1, instance1.getSet(setName).size());
    }

    @Test
    public void testAddExistingElement_withinTxn() throws Exception {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = instance1.getSet(setName);
        set.add(element);

        final TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        assertFalse(txnSet.add(element));

        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testSetSizeAfterAddingDuplicateElement_withinTxn() throws Exception {
        final String element = "item1";
        final String setName = randomString();
        final ISet set = instance1.getSet(setName);
        set.add(element);

        final TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();

        final TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.add(element);
        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testAddRollBack() throws Exception {
        final String setName = randomString();
        final ISet set = instance1.getSet(setName);

        set.add("item1");

        final TransactionContext context = instance1.newTransactionContext();
        context.beginTransaction();
        final TransactionalSet<Object> setTxn = context.getSet(setName);
        setTxn.add("item2");
        context.rollbackTransaction();

        assertEquals(1, set.size());
    }
}
