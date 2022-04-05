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

import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.spi.impl.operationservice.impl.CompletableFutureTestUtil;
import com.hazelcast.spi.impl.operationservice.impl.CompletableFutureTestUtil.CountingExecutor;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static com.hazelcast.spi.impl.operationservice.impl.CompletableFutureTestUtil.ignore;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class CompletableFutureAbstractTest {

    private static final Executor REJECTING_EXECUTOR = new RejectingExecutor();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected final Long returnValue = Long.valueOf(130);
    protected final Object chainedReturnValue = new Object();
    protected CountingExecutor countingExecutor = new CountingExecutor();

    // Creates a new CompletableFuture to be tested
    protected abstract CompletableFuture<Object> newCompletableFuture(boolean exceptional, long completeAfterMillis);

    @Test
    public void thenAccept_onCompletedFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> chained = future.thenAccept(value -> assertEquals(returnValue, value));

        assertTrueEventually(() -> assertTrue(future.isDone()));
        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenAccept_onIncompleteFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Void> chained = future.thenAccept(value -> assertEquals(returnValue, value));

        assertTrueEventually(() -> Assert.assertTrue(chained.isDone()));
        chained.join();
    }

    @Test
    public void thenAcceptAsync_onCompletedFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> chained = future.thenAcceptAsync(value -> assertEquals(returnValue, value));

        assertTrueEventually(() -> Assert.assertTrue(chained.isDone()));
        chained.join();
    }

    @Test
    public void thenAcceptAsync_onIncompleteFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Void> chained = future.thenAcceptAsync(value -> assertEquals(returnValue, value));

        assertTrueEventually(() -> Assert.assertTrue(chained.isDone()));
        chained.join();
    }

    @Test
    public void thenAcceptAsync_withExecutor_onCompletedFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> chained = future.thenAcceptAsync(value -> assertEquals(returnValue, value), countingExecutor);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
        chained.join();
    }

    @Test
    public void thenAcceptAsync_withExecutor_onIncompleteFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Void> chained = future.thenAcceptAsync(value -> assertEquals(returnValue, value), countingExecutor);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
        chained.join();
    }

    @Test
    public void thenAcceptAsync_whenManyChained() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        CompletableFuture<Void> chained1 = future.thenAcceptAsync(value -> assertEquals(returnValue, value), countingExecutor);
        CompletableFuture<Void> chained2 = chained1.thenAcceptAsync(value -> assertNull(value), countingExecutor);

        assertTrueEventually(() -> assertTrue(chained2.isDone()));
        assertTrue(chained1.isDone());
        assertEquals(2, countingExecutor.counter.get());
        chained1.join();
        chained2.join();
    }

    @Test
    public void thenAccept_exceptional() {
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);
        CompletableFuture<Void> chained = future.thenAccept(value -> ignore());

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenAcceptAsync_exceptional() {
        CompletableFuture<Object> future = newCompletableFuture(true, 1000L);
        CompletableFuture<Void> chained = future.thenAcceptAsync(value -> ignore());

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenAccept_onCompletedFuture_whenActionThrowsException() {
        thenAccept_whenActionThrowsException(0L);
    }

    @Test
    public void thenAccept_onIncompleteFuture_whenActionThrowsException() {
        thenAccept_whenActionThrowsException(1000L);
    }

    private void thenAccept_whenActionThrowsException(long completionDelay) {
        CompletableFuture<Object> future = newCompletableFuture(false, completionDelay);
        CompletableFuture<Void> chained = future.thenAccept(value -> {
            throw new ExpectedRuntimeException();
        });

        assertTrueEventually(() -> assertTrue(future.isDone()));
        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertFalse(future.isCompletedExceptionally());
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenApply_whenCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.thenApply(value -> {
            assertEquals(returnValue, value);
            return chainedReturnValue;
        }).toCompletableFuture();

        assertSame(chainedReturnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApply_whenIncompleteFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.thenApply(value -> {
            assertEquals(returnValue, value);
            return chainedReturnValue;
        });

        assertSame(chainedReturnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_whenCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertEquals(returnValue, value);
            return chainedReturnValue;
        }).toCompletableFuture();

        assertSame(chainedReturnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertEquals(returnValue, value);
            return chainedReturnValue;
        }).toCompletableFuture();

        assertSame(chainedReturnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertEquals(returnValue, value);
            return chainedReturnValue;
        }, countingExecutor).toCompletableFuture();

        assertSame(chainedReturnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertEquals(returnValue, value);
            return chainedReturnValue;
        }, countingExecutor).toCompletableFuture();

        assertSame(chainedReturnValue, chained.join());
        assertTrue(chained.isDone());
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenApply_exceptional() {
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);
        CompletableFuture<Object> chained = future.thenApply(Function.identity());

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenApplyAsync_exceptional() {
        CompletableFuture<Object> future = newCompletableFuture(true, 1000L);
        CompletableFuture<Object> chained = future.thenApplyAsync(Function.identity());

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenApply_onCompletedFuture_whenActionThrowsException() {
        thenApply_whenActionThrowsException(0L);
    }

    @Test
    public void thenApply_onIncompleteFuture_whenActionThrowsException() {
        thenApply_whenActionThrowsException(1000L);
    }

    public void thenApply_whenActionThrowsException(long completionDelay) {
        CompletableFuture<Object> future = newCompletableFuture(false, completionDelay);
        CompletableFuture<Void> chained = future.thenApply(value -> {
            throw new ExpectedRuntimeException();
        });

        assertTrueEventually(() -> assertTrue(future.isDone()));
        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertFalse(future.isCompletedExceptionally());
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenRun_whenCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> chained = future.thenRunAsync(CompletableFutureTestUtil::ignore, CALLER_RUNS)
                                                .toCompletableFuture();

        assertTrue(chained.isDone());
    }

    @Test
    public void thenRun_whenIncompleteFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Void> chained = future.thenRun(CompletableFutureTestUtil::ignore).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_whenCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> chained = future.thenRunAsync(CompletableFutureTestUtil::ignore).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Void> chained = future.thenRunAsync(CompletableFutureTestUtil::ignore).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> chained = future.thenRunAsync(CompletableFutureTestUtil::ignore, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Void> chained = future.thenRunAsync(CompletableFutureTestUtil::ignore, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenRunAsync_whenChained() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Void> chained = future.thenRunAsync(CompletableFutureTestUtil::ignore, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenRun_exceptional() {
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);
        CompletableFuture<Void> chained = future.thenRun(CompletableFutureTestUtil::ignore);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenRunAsync_exceptional() {
        CompletableFuture<Object> future = newCompletableFuture(true, 1000L);
        CompletableFuture<Void> chained = future.thenRunAsync(CompletableFutureTestUtil::ignore);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenRun_whenActionThrowsException() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> chained = future.thenRun(() -> {
            throw new IllegalStateException();
        });

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(IllegalStateException.class));
        chained.join();
    }

    @Test
    public void thenRun_onCompletedFuture_whenActionThrowsException() {
        thenRun_whenActionThrowsException(0L);
    }

    @Test
    public void thenRun_onIncompleteFuture_whenActionThrowsException() {
        thenRun_whenActionThrowsException(1000L);
    }

    private void thenRun_whenActionThrowsException(long completionDelay) {
        CompletableFuture<Object> future = newCompletableFuture(false, completionDelay);
        CompletableFuture<Void> chained = future.thenRun(() -> {
            throw new ExpectedRuntimeException();
        });

        assertTrueEventually(() -> assertTrue(future.isDone()));
        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertFalse(future.isCompletedExceptionally());
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void whenComplete_whenCompletedFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertEquals(returnValue, v);
            assertNull(t);
        }, CALLER_RUNS);

        assertTrue(chained.isDone());
        assertEquals(returnValue, chained.join());
    }

    @Test
    public void whenComplete_whenIncompleteFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            assertEquals(returnValue, v);
            assertNull(t);
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(returnValue, chained.join());
    }

    @Test
    public void whenCompleteAsync_whenCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void whenCompleteAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void whenCompleteAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void whenCompleteAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void whenCompleteAsync_whenChained() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void whenComplete_exceptional() {
        CountDownLatch executed = new CountDownLatch(1);
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            assertNull(v);
            assertInstanceOf(ExpectedRuntimeException.class, t);
            executed.countDown();
        });

        assertOpenEventually(executed);
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
    }

    @Test
    public void whenCompleteAsync_exceptional() {
        CountDownLatch executed = new CountDownLatch(1);
        CompletionStage<Object> future = newCompletableFuture(true, 1000L);
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            assertNull(v);
            assertInstanceOf(ExpectedRuntimeException.class, t);
            executed.countDown();
        }).toCompletableFuture();

        assertOpenEventually(executed);
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
    }

    @Test
    public void whenComplete_withExceptionFromBiConsumer() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            throw new ExpectedRuntimeException();
        });
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void whenComplete_withExceptionFromFirstStage_failsWithFirstException() {
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            throw new IllegalArgumentException();
        });
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void whenCompleteAsync_withExceptionFromBiConsumer() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            throw new ExpectedRuntimeException();
        });
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void whenCompleteAsync_withExceptionFromFirstStage_failsWithFirstException() {
        CompletableFuture<Object> future = newCompletableFuture(true, 1000L);
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            throw new IllegalArgumentException();
        });
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void whenCompleteAsync_withExecutor_withExceptionFromBiConsumer() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            throw new ExpectedRuntimeException();
        }, countingExecutor);
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void whenCompleteAsync_withExecutor_withExceptionFromFirstStage_failsWithFirstException() {
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            throw new IllegalArgumentException();
        }, countingExecutor);
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void handle_whenCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> chainedReturnValue, CALLER_RUNS)
                                                  .toCompletableFuture();

        assertTrue(chained.isDone());
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handle_whenIncompleteFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.handle((v, t) -> chainedReturnValue).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_whenCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> chainedReturnValue).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> chainedReturnValue).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            return chainedReturnValue;
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            return chainedReturnValue;
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_whenChained() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            return chainedReturnValue;
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void handle_exceptional() {
        CountDownLatch executed = new CountDownLatch(1);
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);
        CompletableFuture<Object> chained = future.handle((v, t) -> {
            assertNull(v);
            assertInstanceOf(ExpectedRuntimeException.class, t);
            executed.countDown();
            return returnValue;
        });

        assertOpenEventually(executed);
        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertFalse(chained.isCompletedExceptionally());
        assertEquals(returnValue, chained.join());
    }

    @Test
    public void handleAsync_exceptional() {
        CountDownLatch executed = new CountDownLatch(1);
        CompletableFuture<Object> future = newCompletableFuture(true, 1000L);
        CompletableFuture<Object> chained = future.handle((v, t) -> {
            assertNull(v);
            assertInstanceOf(ExpectedRuntimeException.class, t);
            executed.countDown();
            return returnValue;
        });

        assertOpenEventually(executed);
        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertFalse(chained.isCompletedExceptionally());
        assertEquals(returnValue, chained.join());
    }

    @Test
    public void handle_withExceptionFromBiFunction() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.handle((v, t) -> {
            throw new ExpectedRuntimeException();
        });
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    // since handle* methods process and substitute processing outcome of first stage,
    // if the the handler BiFunction fails with an exception, the chained CompletionStage
    // will fail with the exception thrown from the handler's body (not the one thrown from
    // the original CompletionStage).
    @Test
    public void handle_withExceptionFromFirstStage_failsWithSecondException() {
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);
        CompletableFuture<Object> chained = future.handle((v, t) -> {
            throw new IllegalArgumentException();
        });
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(IllegalArgumentException.class));
        chained.join();
    }

    @Test
    public void handleAsync_withExceptionFromBiFunction() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            throw new ExpectedRuntimeException();
        });
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void handleAsync_withExceptionFromFirstStage_failsWithSecondException() {
        CompletableFuture<Object> future = newCompletableFuture(true, 1000L);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            throw new IllegalArgumentException();
        });
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(IllegalArgumentException.class));
        chained.join();
    }

    @Test
    public void handleAsync_withExecutor_withExceptionFromBiFunction() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            throw new ExpectedRuntimeException();
        }, countingExecutor);
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void handleAsync_withExecutor_withExceptionFromFirstStage_failsWithSecondException() {
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            throw new IllegalArgumentException();
        }, countingExecutor);
        assertTrueEventually(() -> assertTrue(chained.isCompletedExceptionally()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(IllegalArgumentException.class));
        chained.join();
    }

    @Test
    public void exceptionally() {
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);
        CompletableFuture<Object> chained = future.exceptionally(t -> chainedReturnValue);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void exceptionally_whenAsync() {
        CompletableFuture<Object> future = newCompletableFuture(true, 1000L);
        CompletableFuture<Object> chained = future.exceptionally(t -> chainedReturnValue);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void exceptionally_whenExceptionFromExceptionallyFunction() {
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);
        CompletableFuture<Object> chained = future.exceptionally(t -> {
            throw new IllegalArgumentException();
        });

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(IllegalArgumentException.class));
        chained.join();
    }

    @Test
    public void exceptionally_whenAsync_andExceptionFromExceptionallyFunction() {
        CompletableFuture<Object> future = newCompletableFuture(true, 1000L);
        CompletableFuture<Object> chained = future.exceptionally(t -> {
            throw new IllegalArgumentException();
        });

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(IllegalArgumentException.class));
        chained.join();
    }

    @Test
    public void exceptionally_whenCompletedNormally() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> chained = future.exceptionally(t -> {
            throw new IllegalArgumentException();
        });

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(future.join(), chained.join());
    }

    @Test
    public void thenCompose_onCompletedFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> composedFuture = future
                .thenCompose(value -> CompletableFuture.completedFuture(returnValue));

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
    }

    @Test
    public void thenCompose_onIncompleteFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> composedFuture = future
                .thenCompose(value -> CompletableFuture.completedFuture(returnValue));

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
    }

    @Test
    public void thenComposeAsync_onCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> composedFuture = future.toCompletableFuture()
                                                         .thenComposeAsync(value -> CompletableFuture.completedFuture(returnValue));

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
    }

    @Test
    public void thenComposeAsync_onIncompleteFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> composedFuture = future
                .thenComposeAsync(value -> CompletableFuture.completedFuture(returnValue));

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
    }

    @Test
    public void thenComposeAsync_withExecutor_onCompletedFuture() {
        CompletionStage<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> composedFuture = future.toCompletableFuture()
                                                         .thenComposeAsync(value -> CompletableFuture.completedFuture(returnValue), countingExecutor);

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenComposeAsync_withExecutor_onIncompleteFuture() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Object> composedFuture = future
                .thenComposeAsync(value -> CompletableFuture.completedFuture(returnValue), countingExecutor);

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test(expected = NullPointerException.class)
    public void thenCompose_whenNullFunction() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        future.thenCompose(null);
    }

    @Test(expected = NullPointerException.class)
    public void thenComposeAsync_whenNullFunction() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        future.thenComposeAsync(null);
    }

    @Test(expected = NullPointerException.class)
    public void thenComposeAsync_withExecutor_whenNullFunction() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        future.thenComposeAsync(null, countingExecutor);
    }

    @Test
    public void thenCompose_whenExceptionFromFirstStage() {
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        future.thenCompose(v -> null).join();
    }

    @Test
    public void thenCompose_whenExceptionFromUserFunction() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(IllegalStateException.class));
        future.thenCompose(v -> {
            throw new IllegalStateException();
        }).join();
    }

    @Test
    public void thenCompose_whenExceptionFromFirstStageAndUserFunction_thenFirstStageExceptionBubbles() {
        CompletableFuture<Object> future = newCompletableFuture(true, 0L);

        expectedException.expect(CompletionException.class);
        // expect the exception thrown from first future
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        future.thenCompose(v -> {
            throw new IllegalStateException();
        }).join();
    }

    @Test
    public void testWhenComplete_whenCancelled() {
        CompletableFuture<Object> future = newCompletableFuture(false, 10000L);
        assertTrue(future.cancel(true));

        CompletableFuture<Object> nextStage = future.whenComplete((v, t) -> {
            assertInstanceOf(CancellationException.class, t);
        });

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(CancellationException.class));
        nextStage.join();
    }

    // Tests for exceptional completion of dependent stage due to executor rejecting execution
    @Test
    public void thenRunAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenRunAsync(() -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void thenRunAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenRunAsync(() -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void thenApplyAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenApplyAsync(v -> null, REJECTING_EXECUTOR).join();
    }

    @Test
    public void thenApplyAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenApplyAsync(v -> null, REJECTING_EXECUTOR).join();
    }

    @Test
    public void thenAcceptAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenAcceptAsync(v -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void thenAcceptAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenAcceptAsync(v -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void handleAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.handleAsync((v, t) -> null, REJECTING_EXECUTOR).join();
    }

    @Test
    public void handleAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.handleAsync((v, t) -> null, REJECTING_EXECUTOR).join();
    }

    @Test
    public void whenCompleteAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.whenCompleteAsync((v, t) -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void whenCompleteAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.whenCompleteAsync((v, t) -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void acceptEitherAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.acceptEitherAsync(newCompletedFuture(null), v -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void acceptEitherAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.acceptEitherAsync(newCompletedFuture(null), v -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void applyToEither() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Long> nextStage = future.applyToEither(new CompletableFuture<Long>(),
              (v) -> {
                  assertEquals(returnValue, v);
                  return 1L;
              });

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        assertEquals(1L, nextStage.join().longValue());
    }

    @Test
    public void applyToEitherAsync() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Long> nextStage = future.applyToEitherAsync(new CompletableFuture<Long>(),
              (v) -> {
                  assertEquals(returnValue, v);
                  return 1L;
              });

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        assertEquals(1L, nextStage.join().longValue());
    }

    @Test
    public void applyToEitherAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.applyToEitherAsync(newCompletedFuture(null), v -> null, REJECTING_EXECUTOR).join();
    }

    @Test
    public void applyToEitherAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.applyToEitherAsync(newCompletedFuture(null), v -> null, REJECTING_EXECUTOR).join();
    }

    @Test
    public void applyToEither_whenActionThrowsException() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Long> nextStage = future.applyToEither(new CompletableFuture<Long>(),
                (v) -> {
            throw new ExpectedRuntimeException();
        });

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        nextStage.join();
    }

    @Test
    public void acceptEither() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> nextStage = future.acceptEither(new CompletableFuture<>(),
              (v) -> assertEquals(returnValue, v));

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        nextStage.join();
    }

    @Test
    public void acceptEitherAsync() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> nextStage = future.acceptEitherAsync(new CompletableFuture<>(),
              (v) -> assertEquals(returnValue, v));

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        nextStage.join();
    }

    @Test
    public void acceptEither_whenActionThrowsException() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);
        CompletableFuture<Void> nextStage = future.acceptEither(new CompletableFuture<>(),
                (v) -> {
            throw new ExpectedRuntimeException();
        });

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        nextStage.join();
    }

    @Test
    public void runAfterBoth() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> nextStage = future.runAfterBoth(newCompletedFuture("otherValue"),
              () -> ignore());

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        nextStage.join();
    }

    @Test
    public void runAfterBothAsync() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> nextStage = future.runAfterBothAsync(newCompletedFuture("otherValue"),
              () -> ignore());

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        nextStage.join();
    }

    @Test
    public void runAfterBothAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.runAfterBothAsync(newCompletedFuture(null), () -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void runAfterBothAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.runAfterBothAsync(newCompletedFuture(null), () -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void runAfterBoth_whenActionThrowsException() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        future.runAfterBoth(newCompletedFuture(null), () -> {
            throw new ExpectedRuntimeException();
        }).join();
    }

    @Test
    public void runAfterEither() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> nextStage = future.runAfterEither(new CompletableFuture<>(),
              () -> ignore());

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
    }

    @Test
    public void runAfterEitherAsync() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> nextStage = future.runAfterEitherAsync(new CompletableFuture<>(),
              () -> ignore());

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
    }

    @Test
    public void runAfterEitherAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.runAfterEitherAsync(newCompletedFuture(null), () -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void runAfterEitherAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.runAfterEitherAsync(newCompletedFuture(null), () -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void runAfterEither_whenActionThrowsException() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        future.runAfterEither(newCompletedFuture(null), () -> {
            throw new ExpectedRuntimeException();
        }).join();
    }

    @Test
    public void thenAcceptBoth() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> nextStage = future.thenAcceptBoth(newCompletedFuture(returnValue),
              (v1, v2) -> assertEquals(returnValue, v1));

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
    }

    @Test
    public void thenAcceptBothAsync() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Void> nextStage = future.thenAcceptBothAsync(newCompletedFuture(returnValue),
              (v1, v2) -> assertEquals(returnValue, v1));

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
    }

    @Test
    public void thenAcceptBothAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenAcceptBothAsync(newCompletedFuture(null), (v, u)  -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void thenAcceptBothAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenAcceptBothAsync(newCompletedFuture(null), (v, u)  -> ignore(), REJECTING_EXECUTOR).join();
    }

    @Test
    public void thenAcceptBoth_whenActionThrowsException() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        future.thenAcceptBoth(newCompletedFuture(null),
                (v, u) -> {
            throw new ExpectedRuntimeException();
        }).join();
    }

    @Test
    public void thenCombine() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> nextStage = future.thenCombine(newCompletedFuture(returnValue),
              (v1, v2) -> v1);

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        assertEquals(returnValue, nextStage.join());
    }

    @Test
    public void thenCombineAsync() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);
        CompletableFuture<Object> nextStage = future.thenCombineAsync(newCompletedFuture(returnValue),
              (v1, v2) -> v1);

        assertTrueEventually(() -> assertTrue(nextStage.isDone()));
        assertEquals(returnValue, nextStage.join());
    }

    @Test
    public void thenCombineAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenCombineAsync(newCompletedFuture(null), (v, u)  -> null, REJECTING_EXECUTOR).join();
    }

    @Test
    public void thenCombineAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenCombineAsync(newCompletedFuture(null), (v, u)  -> null, REJECTING_EXECUTOR).join();
    }

    @Test
    public void thenCombine_whenActionThrowsException() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        future.thenCombine(newCompletedFuture(null), (t, u) -> {
            throw new ExpectedRuntimeException();
        }).join();
    }

    @Test
    public void thenComposeAsync_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 0L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenComposeAsync(v -> newCompletedFuture(null), REJECTING_EXECUTOR).join();
    }

    @Test
    public void thenComposeAsync_onIncompleteFuture_whenExecutionRejected() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(RejectedExecutionException.class));
        future.thenComposeAsync(v -> newCompletedFuture(null), REJECTING_EXECUTOR).join();
    }

    @Test
    public void thenCompose_whenActionThrowsException() {
        CompletableFuture<Object> future = newCompletableFuture(false, 1000L);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        future.thenCompose(v -> {
            throw new ExpectedRuntimeException();
        }).join();
    }

    public static class RejectingExecutor implements Executor {

        @Override
        public void execute(Runnable command) {
            throw new RejectedExecutionException("Execution rejected");
        }
    }
}
