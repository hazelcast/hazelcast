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
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationExecutorImpl_GetOperationRunnerTest extends OperationExecutorImpl_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNull() {
        initExecutor();

        executor.getOperationRunner(null);
    }

    @Test
    public void test_whenCallerIsNormalThread_andGenericOperation_thenReturnAdHocRunner() {
        initExecutor();

        Operation op = new DummyOperation(-1);
        OperationRunner operationRunner = executor.getOperationRunner(op);

        DummyOperationRunnerFactory f = (DummyOperationRunnerFactory) handlerFactory;
        assertSame(f.adhocHandler, operationRunner);
    }

    @Test
    public void test_whenPartitionSpecificOperation_thenReturnCorrectPartitionOperationRunner() {
        initExecutor();

        int partitionId = 0;

        Operation op = new DummyOperation(partitionId);
        OperationRunner runner = executor.getOperationRunner(op);

        assertSame(executor.getPartitionOperationRunners()[partitionId], runner);
    }

    @Test
    public void test_whenCallerIsGenericOperationThread() {
        initExecutor();

        Operation nestedOp = new DummyOperation(-1);

        final GetCurrentThreadOperationHandlerOperation op = new GetCurrentThreadOperationHandlerOperation(nestedOp);
        op.setPartitionId(Operation.GENERIC_PARTITION_ID);

        executor.execute(op);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                boolean found = false;
                OperationRunner foundHandler = op.getResponse();
                for (OperationRunner h : executor.getGenericOperationRunners()) {
                    if (foundHandler == h) {
                        found = true;
                        break;
                    }
                }

                assertTrue("handler is not found is one of the generic handlers", found);
            }
        });
    }

    public class GetCurrentThreadOperationHandlerOperation extends Operation {

        volatile OperationRunner operationRunner;
        final Operation op;

        GetCurrentThreadOperationHandlerOperation(Operation op) {
            this.op = op;
        }

        @Override
        public void run() throws Exception {
            operationRunner = executor.getOperationRunner(op);
        }

        @Override
        public OperationRunner getResponse() {
            return operationRunner;
        }
    }

    class DummyOperation extends Operation {

        DummyOperation(int partitionId) {
            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
        }
    }
}
