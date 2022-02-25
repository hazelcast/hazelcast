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

package com.hazelcast.collection.impl.txnset;

import com.hazelcast.config.Config;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.collection.ISet;
import com.hazelcast.transaction.TransactionalSet;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class TransactionalSetAbstractTest extends HazelcastTestSupport {

    static final String ELEMENT = "item";

    protected HazelcastInstance[] instances;
    protected IAtomicLong atomicLong;
    private ISet<String> set;
    private String setName;
    private HazelcastInstance local;

    @Before
    public void setup() {
        Config config = new Config();
        config.addSetConfig(new SetConfig("testAdd_withMaxCapacity*").setMaxSize(1));

        instances = newInstances(config);
        local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String methodName = getTestMethodName();
        setName = randomNameOwnedBy(target, methodName);
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
    public void testSingleSetAtomicity() throws ExecutionException, InterruptedException {
        final int itemCount = 200;

        Future<Integer> f = spawn(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                ISet<Object> set = local.getSet(setName);
                while (!set.remove("item-1")) {
                }
                return set.size();
            }
        });

        TransactionContext context = local.newTransactionContext();
        context.beginTransaction();

        TransactionalSet<Object> set = context.getSet(setName);
        for (int i = 0; i < itemCount; i++) {
            set.add("item-" + i);
        }
        context.commitTransaction();

        int size = f.get();
        assertEquals(itemCount - 1, size);
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
