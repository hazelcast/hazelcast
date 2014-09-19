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
import com.hazelcast.core.TransactionalList;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TransactionListTest extends HazelcastTestSupport {

    @Test
    public void testAddRemoveList() {
        Config config = new Config();
        final String name = "defList";

        final int insCount = 2;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        TransactionContext context = instances[0].newTransactionContext();
        assertTrue(instances[1].getList(name).add("value1"));
        try {
            context.beginTransaction();

            TransactionalList l = context.getList(name);
            assertEquals(1, l.size());
            assertTrue(l.add("value1"));
            assertEquals(2, l.size());
            assertFalse(l.remove("value2"));
            assertEquals(2, l.size());
            assertTrue(l.remove("value1"));
            assertEquals(1, l.size());
            context.commitTransaction();
        } catch (Exception e) {
            fail(e.getMessage());
            context.rollbackTransaction();
        }

        assertEquals(1, instances[1].getList(name).size());
    }
}
