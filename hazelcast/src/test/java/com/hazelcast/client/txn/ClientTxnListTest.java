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

package com.hazelcast.client.txn;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.IList;
import com.hazelcast.transaction.TransactionalList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author ali 6/11/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTxnListTest {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @Test
    public void testAddRemove() throws Exception {
        String listName = randomString();
        final IList l = client.getList(listName);
        l.add("item1");

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalList<Object> list = context.getList(listName);
        assertTrue(list.add("item2"));
        assertEquals(2, list.size());
        assertEquals(1, l.size());
        assertFalse(list.remove("item3"));
        assertTrue(list.remove("item1"));

        context.commitTransaction();

        assertEquals(1, l.size());
    }

    @Test
    public void testAddAndRoleBack() throws Exception {
        final String listName = randomString();
        final IList l = client.getList(listName);
        l.add("item1");

        final TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        final TransactionalList<Object> list = context.getList(listName);
        list.add("item2");
        context.rollbackTransaction();

        assertEquals(1, l.size());
    }
}
