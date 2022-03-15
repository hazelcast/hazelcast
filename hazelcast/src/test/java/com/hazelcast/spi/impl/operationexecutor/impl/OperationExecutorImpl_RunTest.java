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

import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static com.hazelcast.spi.impl.operationservice.Operation.GENERIC_PARTITION_ID;
import static com.hazelcast.spi.properties.ClusterProperty.GENERIC_OPERATION_THREAD_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.PRIORITY_GENERIC_OPERATION_THREAD_COUNT;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationExecutorImpl_RunTest extends OperationExecutorImpl_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNull() {
        initExecutor();

        executor.run(null);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromNormalThread() {
        initExecutor();

        DummyGenericOperation genericOperation = new DummyGenericOperation();

        executor.run(genericOperation);

        DummyOperationRunner adhocHandler = ((DummyOperationRunnerFactory) handlerFactory).adhocHandler;
        assertContains(adhocHandler.operations, genericOperation);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromGenericThread() {
        config.setProperty(GENERIC_OPERATION_THREAD_COUNT.getName(), "1");
        config.setProperty(PRIORITY_GENERIC_OPERATION_THREAD_COUNT.getName(), "0");
        initExecutor();

        final DummyOperationRunner genericOperationHandler
                = ((DummyOperationRunnerFactory) handlerFactory).genericOperationHandlers.get(0);
        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(GENERIC_PARTITION_ID) {
            @Override
            public Object call() {
                executor.run(genericOperation);
                return null;
            }
        };

        executor.execute(task);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                boolean contains = genericOperationHandler.operations.contains(genericOperation);
                assertTrue("operation is not found in the generic operation handler", contains);
            }
        });
    }

    @Test
    public void test_whenGenericOperation_andCallingFromPartitionThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        final int partitionId = 0;
        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionId) {
            @Override
            public Object call() {
                executor.run(genericOperation);
                return null;
            }
        };

        executor.execute(task);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyOperationRunner handler = (DummyOperationRunner) executor.getPartitionOperationRunners()[partitionId];
                assertContains(handler.operations, genericOperation);
            }
        });
    }

    // IO thread is now allowed to run any operation, so we expect an IllegalThreadStateException
    @Test
    public void test_whenGenericOperation_andCallingFromIOThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    executor.run(genericOperation);
                    return Boolean.FALSE;
                } catch (IllegalThreadStateException e) {
                    return Boolean.TRUE;
                }
            }
        });

        DummyOperationHostileThread thread = new DummyOperationHostileThread(futureTask);
        thread.start();

        assertEqualsEventually(futureTask, Boolean.TRUE);
    }

    @Test(expected = IllegalThreadStateException.class)
    public void test_whenPartitionOperation_andCallingFromNormalThread() {
        initExecutor();

        DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        executor.run(partitionOperation);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromGenericThread() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(GENERIC_PARTITION_ID) {
            @Override
            public Object call() {
                try {
                    executor.run(partitionOperation);
                    return Boolean.FALSE;
                } catch (IllegalThreadStateException e) {
                    return Boolean.TRUE;
                }
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionThread_andWrongPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        int wrongPartitionId = partitionOperation.getPartitionId() + 1;
        PartitionSpecificCallable task = new PartitionSpecificCallable(wrongPartitionId) {
            @Override
            public Object call() {
                try {
                    executor.run(partitionOperation);
                    return Boolean.FALSE;
                } catch (IllegalThreadStateException e) {
                    return Boolean.TRUE;
                }
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionThread_andRightPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        final int partitionId = partitionOperation.getPartitionId();
        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionId) {
            @Override
            public Object call() {
                executor.run(partitionOperation);
                return Boolean.TRUE;
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, Boolean.TRUE);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyOperationRunner handler = (DummyOperationRunner) executor.getPartitionOperationRunners()[partitionId];
                assertContains(handler.operations, partitionOperation);
            }
        });
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromIOThread() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    executor.run(partitionOperation);
                    return Boolean.FALSE;
                } catch (IllegalThreadStateException e) {
                    return Boolean.TRUE;
                }
            }
        });

        DummyOperationHostileThread thread = new DummyOperationHostileThread(futureTask);
        thread.start();

        assertEqualsEventually(futureTask, Boolean.TRUE);
    }
}
