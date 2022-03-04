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

package com.hazelcast.internal.util;

import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
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
        InternalCompletableFuture<String> f = InternalCompletableFuture.newCompletedFuture("result");
        test_completedNormally(f, "result");
    }

    @Test
    public void test_completedNormallyWithNull() throws Exception {
        InternalCompletableFuture<Void> f = InternalCompletableFuture.newCompletedFuture(null);
        test_completedNormally(f, null);
    }

    private <T> void test_completedNormally(InternalCompletableFuture f, T result) throws Exception {
        assertFalse(f.complete("foo"));
        assertFalse(f.cancel(true));
        assertFalse(f.cancel(false));
        assertFalse(f.isCancelled());
        assertTrue(f.isDone());
        assertEquals(result, f.get());
        assertEquals(result, f.get(1, TimeUnit.HOURS));
        assertEquals(result, f.join());

        final int[] intField = {0};
        BiConsumer<T, Throwable> callback = (response, t) -> {
            if (t == null) {
                intField[0]++;
            } else {
                fail("onFailure called: " + t);
            }
        };

        f.whenCompleteAsync(callback, CALLER_RUNS);
        assertEquals(1, intField[0]);

        f.whenCompleteAsync(callback, command -> {
            intField[0]++;
            command.run();
        });
        assertEquals(3, intField[0]);
    }

    @Test
    public void test_completedExceptionallyWithRuntimeException() throws Exception {
        RuntimeException e = new RuntimeException("result");
        InternalCompletableFuture<String> f = InternalCompletableFuture.completedExceptionally(e);
        test_completedExceptionally(f, e);
    }

    @Test
    public void test_completedExceptionallyWithCheckedException() throws Exception {
        Exception e = new Exception("result");
        InternalCompletableFuture<String> f = InternalCompletableFuture.completedExceptionally(e);
        test_completedExceptionally(f, e);
    }

    private <T extends Throwable> void test_completedExceptionally(InternalCompletableFuture<String> f, T result)
                        throws InterruptedException, TimeoutException {
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
        } catch (CompletionException t) {
            assertSame(result, t.getCause());
        }

        final int[] intField = {0};
        BiConsumer<String, Throwable> callback = (response, t) -> {
            if (t == null) {
                fail("onResponse called: " + response);
            } else {
                intField[0]++;
            }
        };

        f.whenCompleteAsync(callback, CALLER_RUNS);
        assertEquals(1, intField[0]);

        f.whenCompleteAsync(callback, command -> {
            intField[0]++;
            command.run();
        });
        assertEquals(3, intField[0]);
    }
}
