/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationExecutorImpl_IsOperationThreadTest extends OperationExecutorImpl_AbstractTest {

    @Test
    public void test_whenCallingFromNonOperationThread() {
        initExecutor();

        boolean result = executor.isOperationThread();

        assertFalse(result);
    }

    @Test
    public void test_whenCallingFromPartitionOperationThread() {
        initExecutor();

        PartitionSpecificCallable task = new PartitionSpecificCallable() {
            @Override
            public Object call() {
                return executor.isOperationThread();
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenCallingFromGenericOperationThread() {
        initExecutor();

        PartitionSpecificCallable task = new PartitionSpecificCallable(Operation.GENERIC_PARTITION_ID) {
            @Override
            public Object call() {
                return executor.isOperationThread();
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }
}
