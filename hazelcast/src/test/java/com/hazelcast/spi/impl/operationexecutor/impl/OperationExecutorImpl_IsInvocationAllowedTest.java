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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationExecutorImpl_IsInvocationAllowedTest extends OperationExecutorImpl_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNullOperation() {
        initExecutor();

        executor.isInvocationAllowed(null, false);
    }

    // ============= generic operations ==============================

    @Test
    public void test_whenGenericOperation_andCallingFromUserThread() {
        initExecutor();

        DummyGenericOperation genericOperation = new DummyGenericOperation();

        boolean result = executor.isInvocationAllowed(genericOperation, false);

        assertTrue(result);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromPartitionOperationThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(0) {
            @Override
            public Object call() {
                return executor.isInvocationAllowed(genericOperation, false);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromGenericOperationThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(Operation.GENERIC_PARTITION_ID) {
            @Override
            public Object call() {
                return executor.isInvocationAllowed(genericOperation, false);
            }
        };
        executor.execute(task);

        assertEqualsEventually(task, TRUE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromOperationHostileThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return executor.isInvocationAllowed(genericOperation, false);
            }
        });

        DummyOperationHostileThread thread = new DummyOperationHostileThread(futureTask);
        thread.start();

        assertEqualsEventually(futureTask, FALSE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromOperationHostileThread_andAsync() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return executor.isInvocationAllowed(genericOperation, true);
            }
        });

        DummyOperationHostileThread hostileThread = new DummyOperationHostileThread(futureTask);
        hostileThread.start();

        assertEqualsEventually(futureTask, FALSE);
    }

    // ===================== partition specific operations ========================

    @Test
    public void test_whenPartitionOperation_andCallingFromUserThread() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        boolean result = executor.isInvocationAllowed(partitionOperation, false);

        assertTrue(result);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromGenericOperationThread() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(Operation.GENERIC_PARTITION_ID) {
            @Override
            public Object call() {
                return executor.isInvocationAllowed(partitionOperation, false);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andCorrectPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionOperation.getPartitionId()) {
            @Override
            public Object call() {
                return executor.isInvocationAllowed(partitionOperation, false);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andWrongPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        int wrongPartition = partitionOperation.getPartitionId() + 1;
        PartitionSpecificCallable task = new PartitionSpecificCallable(wrongPartition) {
            @Override
            public Object call() {
                return executor.isInvocationAllowed(partitionOperation, false);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, FALSE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andWrongPartition_andAsync() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        int wrongPartition = partitionOperation.getPartitionId() + 1;
        PartitionSpecificCallable task = new PartitionSpecificCallable(wrongPartition) {
            @Override
            public Object call() {
                return executor.isInvocationAllowed(partitionOperation, true);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromOperationHostileThread() {
        initExecutor();

        final Operation operation = new DummyOperation(1);

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return executor.isInvocationAllowed(operation, false);
            }
        });

        DummyOperationHostileThread thread = new DummyOperationHostileThread(futureTask);
        thread.start();

        assertEqualsEventually(futureTask, FALSE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromOperationHostileThread_andAsync() {
        initExecutor();

        final Operation operation = new DummyOperation(1);

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return executor.isInvocationAllowed(operation, true);
            }
        });

        DummyOperationHostileThread thread = new DummyOperationHostileThread(futureTask);
        thread.start();

        assertEqualsEventually(futureTask, FALSE);
    }
}
