/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractInvocationFuture_IsDoneTest extends AbstractInvocationFuture_AbstractTest {

    @Test
    public void whenVoid() {
        assertFalse(future.isDone());
    }

    @Test
    public void whenNullResult() {
        future.complete(null);
        assertTrue(future.isDone());
    }

    @Test
    public void whenNoneNullResult() {
        future.complete(value);
        assertTrue(future.isDone());
    }

    @Test
    public void whenExceptionalResult() {
        future.complete(new RuntimeException());
        assertTrue(future.isDone());
    }

    @Test
    public void whenBlockingThread() {
        spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.get();
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotSame(AbstractInvocationFuture.VOID, future.getState());
            }
        });

        assertFalse(future.isDone());
    }

    @Test
    public void whenCallbackWithoutCustomExecutor() {
        future.andThen(mock(ExecutionCallback.class));

        assertFalse(future.isDone());
    }

    @Test
    public void whenCallbackWithCustomExecutor() {
        future.andThen(mock(ExecutionCallback.class), mock(Executor.class));

        assertFalse(future.isDone());
    }

    @Test
    public void whenMultipleWaiters() {
        future.andThen(mock(ExecutionCallback.class), mock(Executor.class));
        future.andThen(mock(ExecutionCallback.class), mock(Executor.class));

        assertFalse(future.isDone());
    }

}
