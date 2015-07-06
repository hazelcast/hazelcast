/*
* Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jca;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
/**
 * This test class is to ensure that lifecycle and transaction methods are not exposed in {@link com.hazelcast.jca.HazelcastConnection}
 */
@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class HazelcastConnectionContractTest {

    static HazelcastConnectionImpl connection;

    @BeforeClass
    public static void setup() {
       connection = new HazelcastConnectionImpl(null,null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLifecycleService() {
        connection.getLifecycleService();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testShutdown() {
        connection.shutdown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testExecuteTransaction() {
        connection.executeTransaction(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testExecuteTransactionTwoArgs() {
        connection.executeTransaction(null,null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNewTransactionContext() {
        connection.newTransactionContext();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNewTransactionContext2() {
        connection.newTransactionContext(null);
    }
}