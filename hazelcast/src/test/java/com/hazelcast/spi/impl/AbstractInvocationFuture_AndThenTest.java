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

package com.hazelcast.spi.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

// todo most are already covered in other tests, to be evaluated
@Ignore
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractInvocationFuture_AndThenTest extends AbstractInvocationFuture_AbstractTest {

    @Test(expected = IllegalArgumentException.class)
    public void whenNullCallback0() {
        future.whenComplete(null);
    }

//    @Test(expected = IllegalArgumentException.class)
//    public void whenNullCallback1() {
//        future.andThen(null, mock(Executor.class));
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void whenNullExecutor() {
//        future.andThen(mock(ExecutionCallback.class), null);
//    }
//
//    @Test
//    public void whenCustomExecutor() {
//        Executor defaultExecutor = mock(Executor.class);
//        Executor customExecutor = mock(Executor.class);
//        TestFuture future = new TestFuture(defaultExecutor, logger);
//        final ExecutionCallback callback = mock(ExecutionCallback.class);
//        future.andThen(callback, customExecutor);
//
//        future.complete(value);
//
//        verify(customExecutor).execute(any(Runnable.class));
//        verifyZeroInteractions(defaultExecutor);
//    }
//
//    @Test
//    public void whenDefaultExecutor() {
//        Executor defaultExecutor = mock(Executor.class);
//        TestFuture future = new TestFuture(defaultExecutor, logger);
//        final ExecutionCallback callback = mock(ExecutionCallback.class);
//        future.andThen(callback);
//
//        future.complete(value);
//
//        verify(defaultExecutor).execute(any(Runnable.class));
//    }
//
//    @Test
//    public void whenResponseAlreadyAvailable() {
//        future.complete(value);
//
//        final ExecutionCallback callback = mock(ExecutionCallback.class);
//        future.andThen(callback);
//
//        assertTrueEventually(() -> verify(callback).onResponse(value));
//    }
//
//    @Test
//    public void whenResponseAvailableAfterSomeWaiting() {
//        final ExecutionCallback callback = mock(ExecutionCallback.class);
//        future.andThen(callback);
//
//        sleepSeconds(5);
//        verifyZeroInteractions(callback);
//
//        future.complete(value);
//
//        assertTrueEventually(() -> verify(callback).onResponse(value));
//    }
//
//    @Test
//    public void whenExceptionalResponseAvailableAfterSomeWaiting() {
//        final ExecutionCallback callback = mock(ExecutionCallback.class);
//        future.andThen(callback);
//
//        sleepSeconds(5);
//        verifyZeroInteractions(callback);
//
//        final ExpectedRuntimeException ex = new ExpectedRuntimeException();
//        future.completeExceptionally(ex);
//
//        assertTrueEventually(() -> verify(callback).onFailure(ex));
//    }
//
//    @Test
//    public void whenMultipleCallbacks() throws ExecutionException, InterruptedException {
//        List<ExecutionCallback> callbacks = new LinkedList<ExecutionCallback>();
//        for (int k = 0; k < 10; k++) {
//            ExecutionCallback callback = mock(ExecutionCallback.class);
//            future.andThen(callback);
//        }
//
//        sleepSeconds(5);
//        future.complete(value);
//
//        for (ExecutionCallback callback : callbacks) {
//            verify(callback).onResponse(value);
//        }
//
//        assertSame(value, future.getState());
//    }
//
//    @Test
//    public void whenExceptionalResponseAvailableAfterSomeWaiting_MemberLeftException() {
//        final ExecutionCallback callback = mock(ExecutionCallback.class);
//        future.andThen(callback);
//
//        final MemberLeftException ex = new MemberLeftException();
//        future.completeExceptionally(ex);
//
//        assertTrueEventually(() -> verify(callback).onFailure(ex));
//    }
}
