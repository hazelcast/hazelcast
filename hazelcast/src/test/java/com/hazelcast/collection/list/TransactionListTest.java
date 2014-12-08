/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.list;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.Member;
import com.hazelcast.core.TransactionalList;
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
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TransactionListTest extends HazelcastTestSupport {

    @Test
    public void testAdd(){
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
    public void testRemove(){
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
    public void testRemove_withNotContainedItem(){
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
    public void testMigrationSerializationNotFails_whenTransactionsAreUsed() throws Exception {
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
            assertEquals(i,l.get(i));
        }
    }

}
