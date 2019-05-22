/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.impl.operations.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicReference;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationExecutorImpl_ExecuteOperationTest extends OperationExecutorImpl_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void whenNull() {
        initExecutor();

        executor.execute((Operation) null);
    }

    @Test
    public void whenPartitionSpecific() {
        initExecutor();

        final AtomicReference<Thread> executingThread = new AtomicReference<Thread>();

        Operation op = new Operation() {
            @Override
            public void run() throws Exception {
                executingThread.set(Thread.currentThread());
            }
        };
        executor.execute(op.setPartitionId(0));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstanceOf(PartitionOperationThread.class, executingThread.get());
            }
        });
    }

    @Test
    public void whenGeneric() {
        initExecutor();

        final AtomicReference<Thread> executingThread = new AtomicReference<Thread>();

        Operation op = new Operation() {
            @Override
            public void run() throws Exception {
                executingThread.set(Thread.currentThread());
            }
        };
        executor.execute(op.setPartitionId(-1));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstanceOf(GenericOperationThread.class, executingThread.get());
            }
        });
    }
}
