package com.hazelcast.collection.impl.txnset;

import com.hazelcast.config.Config;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ISet;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getThreadLocalFrameworkMethod;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public abstract class TransactionalSetBasicTest extends HazelcastTestSupport {

    static final String ELEMENT = "item";

    protected HazelcastInstance[] instances;
    protected IAtomicLong atomicLong;
    private ISet<String> set;
    private SetConfig setConfig;
    private String setName;
    private HazelcastInstance local;

    @Before
    public void setup() {
        Config config = new Config();
        config.addSetConfig(new SetConfig("testAdd_withMaxCapacity*").setMaxSize(1));

        instances = newInstances(config);
        local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String methodName = getThreadLocalFrameworkMethod().getMethod().getName();
        setName = randomNameOwnedBy(target, methodName);
        setConfig = config.getSetConfig(setName);
        set = local.getSet(setName);
    }

    protected abstract HazelcastInstance[] newInstances(Config config);

    @Test
    public void testAdd_withinTxn() throws Exception {
        TransactionContext context = local.newTransactionContext();
        context.beginTransaction();
        TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.add(ELEMENT));
        assertEquals(1, txnSet.size());

        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testSetSizeAfterAdd_withinTxn() throws Exception {
        TransactionContext context = local.newTransactionContext();
        context.beginTransaction();
        TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.add(ELEMENT);

        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testRemove_withinTxn() throws Exception {
        set.add(ELEMENT);

        TransactionContext context = local.newTransactionContext();
        context.beginTransaction();
        TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.remove(ELEMENT));
        assertFalse(txnSet.remove("NOT_THERE"));

        context.commitTransaction();
        assertEquals(0, set.size());

    }

    @Test
    public void testSetSizeAfterRemove_withinTxn() throws Exception {
        set.add(ELEMENT);

        TransactionContext context = local.newTransactionContext();
        context.beginTransaction();
        TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.remove(ELEMENT);
        context.commitTransaction();
        assertEquals(0, set.size());
    }

    @Test
    public void testAddDuplicateElement_withinTxn() throws Exception {
        TransactionContext context = local.newTransactionContext();
        context.beginTransaction();
        TransactionalSet<Object> txnSet = context.getSet(setName);
        assertTrue(txnSet.add(ELEMENT));
        assertFalse(txnSet.add(ELEMENT));
        context.commitTransaction();
        assertEquals(1, local.getSet(setName).size());
    }

    @Test
    public void testAddExistingElement_withinTxn() throws Exception {
        set.add(ELEMENT);

        TransactionContext context = local.newTransactionContext();
        context.beginTransaction();
        TransactionalSet<Object> txnSet = context.getSet(setName);
        assertFalse(txnSet.add(ELEMENT));
        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testSetSizeAfterAddingDuplicateElement_withinTxn() throws Exception {
        set.add(ELEMENT);

        TransactionContext context = local.newTransactionContext();
        context.beginTransaction();
        TransactionalSet<Object> txnSet = context.getSet(setName);
        txnSet.add(ELEMENT);
        context.commitTransaction();
        assertEquals(1, set.size());
    }

    @Test
    public void testAddRollBack() throws Exception {
        set.add(ELEMENT);

        TransactionContext context = local.newTransactionContext();
        context.beginTransaction();
        TransactionalSet<Object> setTxn = context.getSet(setName);
        setTxn.add("itemWillGetRollBacked");
        context.rollbackTransaction();
        assertEquals(1, set.size());
    }
}
