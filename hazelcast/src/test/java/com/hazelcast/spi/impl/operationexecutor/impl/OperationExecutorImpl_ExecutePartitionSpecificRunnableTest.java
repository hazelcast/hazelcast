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

import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicReference;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationExecutorImpl_ExecutePartitionSpecificRunnableTest extends OperationExecutorImpl_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void whenNull() {
        initExecutor();

        executor.execute((PartitionSpecificRunnable) null);
    }

    @Test
    public void whenPartitionSpecific() {
        initExecutor();

        final AtomicReference<Thread> executingThead = new AtomicReference<Thread>();

        PartitionSpecificRunnable task = new PartitionSpecificRunnable() {
            @Override
            public void run() {
                executingThead.set(Thread.currentThread());
            }

            @Override
            public int getPartitionId() {
                return 0;
            }
        };
        executor.execute(task);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstanceOf(PartitionOperationThread.class, executingThead.get());
            }
        });
    }

    @Test
    public void whenGeneric() {
        initExecutor();

        final AtomicReference<Thread> executingThead = new AtomicReference<Thread>();

        PartitionSpecificRunnable task = new PartitionSpecificRunnable() {
            @Override
            public void run() {
                executingThead.set(Thread.currentThread());
            }

            @Override
            public int getPartitionId() {
                return -1;
            }
        };
        executor.execute(task);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstanceOf(GenericOperationThread.class, executingThead.get());
            }
        });
    }
}
