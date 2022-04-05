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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTransactionTest extends HazelcastTestSupport {

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


    @Test(expected = TransactionException.class)
    public void testTxnRollback_WithExecuteTask_throwsTransactionException() {
        client.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext context) throws TransactionException {
                throw new TransactionException();
            }
        });
    }

    @Test(expected = TransactionException.class)
    public void testTxnRollback_WithExecuteTask_throwsTransactionExceptionAsCause() {
        client.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext context) throws TransactionException {
                throw new RuntimeException(new TransactionException());
            }
        });
    }

    @Test(expected = RuntimeException.class)
    public void testTxnRollback_WithExecuteTask_throwsRuntimeException() {
        client.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext context) throws TransactionException {
                throw new RuntimeException();
            }
        });
    }

    @Test(expected = TransactionException.class)
    public void testTxnRollback_WithExecuteTask_throwsError() {
        client.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext context) throws TransactionException {
                throw new Error();
            }
        });
    }

}
