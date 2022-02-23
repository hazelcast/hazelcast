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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Test DelegatingCompletableFuture dependent actions when these fail
 * with HazelcastSerializationException.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DelegatingCompletableFuture_SerializationExceptionTest {

    protected final Data invalidData = new HeapData(new byte[] {0, 0, 0, 0, 5, 0, 0, 0, 0});

    private InternalSerializationService serializationService
            = new DefaultSerializationServiceBuilder().build();
    private AtomicBoolean executed = new AtomicBoolean();

    @Test
    public void ensureInvalidData() {
        assertThrows(HazelcastSerializationException.class,
                () -> serializationService.toObject(invalidData));
    }

    @Test
    public void test_isCompletedExceptionally() {
        CompletableFuture<Object> future = newCompletableFuture(0L);

        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    public void test_get() {
        CompletableFuture<Object> future = newCompletableFuture(0L);
        Throwable t = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(HazelcastSerializationException.class, t.getCause());
    }

    @Test
    public void test_getWithTimeout() {
        CompletableFuture<Object> future = newCompletableFuture(0L);
        Throwable t = assertThrows(ExecutionException.class,
                () -> future.get(1, TimeUnit.SECONDS));
        assertInstanceOf(HazelcastSerializationException.class, t.getCause());
    }

    @Test
    public void test_getNow() {
        CompletableFuture<Object> future = newCompletableFuture(0L);
        Throwable t = assertThrows(CompletionException.class,
                () -> future.getNow(null));
        assertInstanceOf(HazelcastSerializationException.class, t.getCause());
    }

    @Test
    public void test_join() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0L);
        Throwable t = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(HazelcastSerializationException.class, t.getCause());
    }

    @Test
    public void test_joinInternal() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0L);
        assertThrows(HazelcastSerializationException.class, future::joinInternal);
    }

    @Test
    public void test_thenApply() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0L);
        CompletableFuture<Object> chained = future.thenApply(v -> {
            executed.set(true);
            return new Object();
        });

        assertWithCause(chained);
        assertFalse(executed.get());
    }

    @Test
    public void test_thenAccept() {
        InternalCompletableFuture<Object> future = newCompletableFuture(1000L);
        CompletableFuture<Void> chained = future.thenAccept(v -> executed.set(true));

        assertWithCause(chained);
        assertFalse(executed.get());
    }

    @Test
    public void test_thenRun() {
        InternalCompletableFuture<Object> future = newCompletableFuture(1000L);
        CompletableFuture<Void> chained = future.thenRun(() -> executed.set(true));

        assertWithCause(chained);
        assertFalse(executed.get());
    }

    @Test
    public void test_thenCombine() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0);
        CompletableFuture<Object> chained = future.thenCombine(newCompletableFuture(1000), (v, t) -> {
            executed.set(true);
            return new Object();
        });

        assertWithCause(chained);
        assertFalse(executed.get());
    }

    @Test
    public void test_thenAcceptBoth() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0);
        CompletableFuture<Void> chained = future.thenAcceptBoth(newCompletableFuture(1000),
                (v, t) -> executed.set(true));

        assertWithCause(chained);
        assertFalse(executed.get());
    }

    @Test
    public void test_runAfterBoth() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0);
        CompletableFuture<Void> chained = future.runAfterBoth(newCompletableFuture(1000),
                () -> executed.set(true));

        assertWithCause(chained);
        assertFalse(executed.get());
    }

    @Test
    public void test_applyToEither() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0);
        CompletableFuture<Object> chained = future.applyToEither(newCompletableFuture(1000),
                v -> {
                    executed.set(true);
                    return new Object();
                });

        assertWithCause(chained);
        assertFalse(executed.get());
    }

    @Test
    public void test_acceptEither() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0);
        CompletableFuture<Void> chained = future.acceptEither(newCompletableFuture(1000),
                v -> executed.set(true));

        assertWithCause(chained);
        assertFalse(executed.get());
    }

    @Test
    public void test_runAfterEither() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0);
        CompletableFuture<Void> chained = future.runAfterEither(newCompletableFuture(1000),
                () -> executed.set(true));

        assertWithCause(chained);
        assertFalse(executed.get());
    }

    @Test
    public void test_thenCompose() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0);
        CompletableFuture<Object> chained = future.thenCompose(v -> {
            executed.set(true);
            return CompletableFuture.completedFuture(new Object());
        });

        assertWithCause(chained);
        assertFalse(executed.get());
    }

    @Test
    public void test_whenComplete() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0);
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            executed.set(true);
            assertNull(v);
            assertInstanceOf(HazelcastSerializationException.class, t);
        });

        assertWithCause(chained);
        assertTrue(executed.get());
    }

    @Test
    public void test_handle() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0);
        Object chainedReturnValue = new Object();
        CompletableFuture<Object> chained = future.handle((v, t) -> {
            executed.set(true);
            assertNull(v);
            assertInstanceOf(HazelcastSerializationException.class, t);
            return chainedReturnValue;
        });

        assertEquals(chainedReturnValue, chained.join());
        assertTrue(executed.get());
    }

    @Test
    public void test_exceptionally() {
        InternalCompletableFuture<Object> future = newCompletableFuture(0);
        Object chainedReturnValue = new Object();
        CompletableFuture<Object> chained = future.exceptionally(t -> {
            executed.set(true);
            assertInstanceOf(HazelcastSerializationException.class, t);
            return chainedReturnValue;
        });

        assertEquals(chainedReturnValue, chained.join());
        assertTrue(executed.get());
    }

    protected InternalCompletableFuture<Object> newCompletableFuture(long completeAfterMillis) {
        InternalCompletableFuture<Object> future = new InternalCompletableFuture<>();
        Executor completionExecutor;
        if (completeAfterMillis <= 0) {
            completionExecutor = CALLER_RUNS;
        } else {
            completionExecutor = command -> new Thread(() -> {
                sleepAtLeastMillis(completeAfterMillis);
                command.run();
            }, "test-completion-thread").start();
        }
        completionExecutor.execute(() -> future.complete(invalidData));
        return new DelegatingCompletableFuture<>(serializationService, future);
    }

    private void assertWithCause(CompletableFuture<?> future) {
        Throwable t = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(HazelcastSerializationException.class, t.getCause());
    }
}
