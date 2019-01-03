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

package com.hazelcast.internal.util;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SimpleCompletedFutureTest {

    @Test
    public void test_completedNormallyWithValue() throws Exception {
        SimpleCompletedFuture<String> f = new SimpleCompletedFuture<String>("result");
        test_completedNormally(f, "result");
    }

    @Test
    public void test_completedNormallyWithNull() throws Exception {
        SimpleCompletedFuture<Void> f = new SimpleCompletedFuture<Void>((Void) null);
        test_completedNormally(f, null);
    }

    private <T> void test_completedNormally(SimpleCompletedFuture<T> f, T result) throws Exception {
        assertFalse(f.complete("foo"));
        assertFalse(f.cancel(true));
        assertFalse(f.cancel(false));
        assertFalse(f.isCancelled());
        assertTrue(f.isDone());
        assertEquals(result, f.get());
        assertEquals(result, f.get(1, TimeUnit.HOURS));
        assertEquals(result, f.join());

        final int[] intField = {0};
        ExecutionCallback<T> callback = new ExecutionCallback<T>() {
            @Override
            public void onResponse(T response) {
                intField[0]++;
            }

            @Override
            public void onFailure(Throwable t) {
                fail("onFailure called: " + t);
            }
        };

        f.andThen(callback);
        assertEquals(1, intField[0]);

        f.andThen(callback, new Executor() {
            @Override
            public void execute(Runnable command) {
                intField[0]++;
                command.run();
            }
        });
        assertEquals(3, intField[0]);
    }

    @Test
    public void test_completedExceptionallyWithRuntimeException() throws Exception {
        RuntimeException e = new RuntimeException("result");
        SimpleCompletedFuture<String> f = new SimpleCompletedFuture<String>(e);
        test_completedExceptionally(f, e);
    }

    @Test
    public void test_completedExceptionallyWithCheckedException() throws Exception {
        Exception e = new Exception("result");
        SimpleCompletedFuture<String> f = new SimpleCompletedFuture<String>(e);
        test_completedExceptionally(f, e);
    }

    private <T extends Throwable> void test_completedExceptionally(SimpleCompletedFuture<String> f, T result) {
        assertFalse(f.complete("foo"));
        assertFalse(f.cancel(true));
        assertFalse(f.cancel(false));
        assertEquals(result instanceof CancellationException, f.isCancelled());
        assertTrue(f.isDone());
        try {
            f.get();
            fail("should have thrown");
        } catch (ExecutionException e) {
            assertSame(result, e.getCause());
        }
        try {
            f.get(1, TimeUnit.HOURS);
            fail("should have thrown");
        } catch (ExecutionException e) {
            assertSame(result, e.getCause());
        }
        try {
            f.join();
            fail("should have thrown");
        } catch (Throwable t) {
            assertSame(result, t);
        }

        final int[] intField = {0};
        ExecutionCallback<String> callback = new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
                fail("onResponse called: " + response);
            }

            @Override
            public void onFailure(Throwable t) {
                intField[0]++;
            }
        };

        f.andThen(callback);
        assertEquals(1, intField[0]);

        f.andThen(callback, new Executor() {
            @Override
            public void execute(Runnable command) {
                intField[0]++;
                command.run();
            }
        });
        assertEquals(3, intField[0]);
    }
}
