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

import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractInvocationFuture_GetSafely extends AbstractInvocationFuture_AbstractTest {

    @Test
    public void whenNormalResponse() throws ExecutionException, InterruptedException {
        future.complete(value);

        Future joinFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.join();
            }
        });

        assertCompletesEventually(joinFuture);
        assertSame(value, joinFuture.get());
    }

    @Test
    public void whenRuntimeException() throws Exception {
        ExpectedRuntimeException ex = new ExpectedRuntimeException();
        future.completeExceptionally(ex);

        Future joinFuture = spawn(() -> future.join());

        assertCompletesEventually(joinFuture);
        try {
            joinFuture.get();
            fail();
        } catch (ExecutionException e) {
            CompletionException wrapper = assertInstanceOf(CompletionException.class, e.getCause());
            assertSame(ex, wrapper.getCause());
        }
    }

    @Test
    public void whenRegularException() throws Exception {
        Exception ex = new Exception();
        future.completeExceptionally(ex);

        Future joinFuture = spawn(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return future.join();
            }
        });

        assertCompletesEventually(joinFuture);
        try {
            joinFuture.get();
            fail();
        } catch (ExecutionException e) {
            CompletionException wrapper = assertInstanceOf(CompletionException.class, e.getCause());
            assertSame(ex, wrapper.getCause());
        }
    }
}
