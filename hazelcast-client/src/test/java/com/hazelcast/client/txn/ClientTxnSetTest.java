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

package com.hazelcast.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

/**
 * @author ali 6/11/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnSetTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;

    @BeforeClass
    public static void init(){
        server = Hazelcast.newHazelcastInstance();
        hz = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        hz.shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testAddRemove() throws Exception {
        final ISet s = hz.getSet(name);
        s.add("item1");

        final TransactionContext context = hz.newTransactionContext();
        context.beginTransaction();
        final TransactionalSet<Object> set = context.getSet(name);
        assertTrue(set.add("item2"));
        assertEquals(2, set.size());
        assertEquals(1, s.size());
        assertFalse(set.remove("item3"));
        assertTrue(set.remove("item1"));

        context.commitTransaction();

        assertEquals(1, s.size());

    }
}
