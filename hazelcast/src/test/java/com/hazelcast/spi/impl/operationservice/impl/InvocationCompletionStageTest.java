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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.operationservice.impl.CompletableFutureTestUtil.CountingExecutor;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RootCauseMatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static com.hazelcast.spi.impl.operationservice.impl.CompletableFutureTestUtil.invokeAsync;
import static com.hazelcast.spi.impl.operationservice.impl.CompletableFutureTestUtil.invokeSync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link CompletionStage} implementation of {@link InvocationFuture}.
 * Each {@code then*} method ({@code thenApply}, {@code thenAccept}, {@code thenRun}) is tested:
 * <ul>
 *     <li>across all method variants: plain, async, async with explicit executor as argument</li>
 *     <li>as a stage following a future that at the time of registration is either incomplete or completed</li>
 * </ul>
 */
// todo test exceptions from user customization implementations (eg more like thenRun_whenExceptionThrownFromRunnable)
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationCompletionStageTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final Object returnValue = new Object();
    private final Object chainedReturnValue = new Object();

    private CountingExecutor countingExecutor;
    private HazelcastInstance local;
    private OperationServiceImpl operationService;
    private final ILogger logger = Logger.getLogger(InvocationCompletionStageTest.class);

    @Before
    public void setup() {
        local = createHazelcastInstance();
        operationService = getOperationService(local);
        countingExecutor = new CountingExecutor();
    }

    @Test
    public void thenAccept_onCompletedFuture() {
        CompletableFuture<Object> future = invokeSync(local, false);

        CompletableFuture<Void> chained = prepareThenAccept(future, false, false);

        assertTrueEventually(() -> assertTrue(future.isDone()));
        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenAccept_onIncompleteFuture() {
        CompletableFuture<Object> future = invokeAsync(local, false);

        CompletableFuture<Void> chained = prepareThenAccept(future, false, false);

        assertTrueEventually(() -> Assert.assertTrue(chained.isDone()));
    }

    @Test
    public void thenAcceptAsync_onCompletedFuture() {
        CompletableFuture<Object> future = invokeSync(local, false);
        CompletableFuture<Void> chained = prepareThenAccept(future, true, false);
        assertTrueEventually(() -> Assert.assertTrue(chained.isDone()));
    }

    @Test
    public void thenAcceptAsync_onIncompleteFuture() {
        CompletableFuture<Object> future = invokeAsync(local, false);
        CompletableFuture<Void> chained = prepareThenAccept(future, true, false);
        assertTrueEventually(() -> Assert.assertTrue(chained.isDone()));
    }

    @Test
    public void thenAcceptAsync_withExecutor_onCompletedFuture() {
        CompletableFuture<Object> future = invokeSync(local, false);
        CompletableFuture<Void> chained = prepareThenAccept(future, true, true);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenAcceptAsync_withExecutor_onIncompleteFuture() {
        CompletableFuture<Object> future = invokeAsync(local, false);
        CompletableFuture<Void> chained = prepareThenAccept(future, true, true);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenAcceptAsync_whenManyChained() {
        CompletableFuture<Object> future = invokeAsync(local, false);

        CompletableFuture<Void> chained1 = prepareThenAccept(future, true, true);
        CompletableFuture<Void> chained2 = prepareThenAccept(chained1, true, true);

        assertTrueEventually(() -> assertTrue(chained2.isDone()));
        assertTrue(chained1.isDone());
        assertEquals(2, countingExecutor.counter.get());
    }

    @Test
    public void thenAccept_exceptional() {
        CompletableFuture<Object> future = invokeSync(local, true);
        CompletableFuture<Void> chained = prepareThenAccept(future, false, false);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenAcceptAsync_exceptional() {
        CompletableFuture<Object> future = invokeAsync(local, true);
        CompletableFuture<Void> chained = prepareThenAccept(future, true, false);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenApply_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Object> chained = future.thenApply(value -> {
            assertNull(value);
            return returnValue;
        }).toCompletableFuture();

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApply_whenIncompleteFuture() {
        CompletableFuture<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> chained = future.thenApply(value -> {
            assertNull(value);
            return returnValue;
        });

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertNull(value);
            return returnValue;
        }).toCompletableFuture();

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertNull(value);
            return returnValue;
        }).toCompletableFuture();

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertNull(value);
            return returnValue;
        }, countingExecutor).toCompletableFuture();

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertNull(value);
            return returnValue;
        }, countingExecutor).toCompletableFuture();

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenApply_exceptional() {
        CompletableFuture<Object> future = invokeSync(local, true);
        CompletableFuture<Object> chained = future.thenApply(Function.identity());

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenApplyAsync_exceptional() {
        CompletableFuture<Object> future = invokeAsync(local, true);
        CompletableFuture<Object> chained = future.thenApplyAsync(Function.identity());

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenRun_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Void> chained = future.thenRun(this::ignore).toCompletableFuture();

        assertTrue(chained.isDone());
    }

    @Test
    public void thenRun_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Void> chained = future.thenRun(this::ignore).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Void> chained = future.thenRunAsync(this::ignore).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Void> chained = future.thenRunAsync(this::ignore).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Void> chained = future.thenRunAsync(this::ignore, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Void> chained = future.thenRunAsync(this::ignore, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenRunAsync_whenChained() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Void> chained = future.thenRunAsync(this::ignore, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenRun_exceptional() {
        CompletableFuture<Object> future = invokeSync(local, true);
        CompletableFuture<Void> chained = future.thenRun(this::ignore);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenRunAsync_exceptional() {
        CompletableFuture<Object> future = invokeAsync(local, true);
        CompletableFuture<Void> chained = future.thenRunAsync(this::ignore);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertTrue(chained.isCompletedExceptionally());
        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        chained.join();
    }

    @Test
    public void thenRun_whenExceptionThrownFromRunnable() {
        CompletableFuture<Object> future = invokeSync(local, false);
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
    public void whenComplete_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            assertNull(v);
            assertNull(t);
        }).toCompletableFuture();

        assertTrue(chained.isDone());
        assertNull(chained.join());
    }

    @Test
    public void whenComplete_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            assertNull(v);
            assertNull(t);
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertNull(chained.join());
    }

    @Test
    public void whenCompleteAsync_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void whenCompleteAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void whenCompleteAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void whenCompleteAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void whenCompleteAsync_whenChained() {
        CompletionStage<Object> future = invokeAsync(local, false);
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
        CompletableFuture<Object> future = invokeSync(local, true);
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
        CompletionStage<Object> future = invokeAsync(local, true);
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
        CompletableFuture<Object> future = invokeSync(local, false);
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
        CompletableFuture<Object> future = invokeSync(local, true);
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
        CompletableFuture<Object> future = invokeAsync(local, false);
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
        CompletableFuture<Object> future = invokeAsync(local, true);
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
        CompletableFuture<Object> future = invokeSync(local, false);
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
        CompletableFuture<Object> future = invokeSync(local, true);
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
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Object> chained = future.handle((v, t) -> chainedReturnValue).toCompletableFuture();

        assertTrue(chained.isDone());
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handle_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> chained = future.handle((v, t) -> chainedReturnValue).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> chainedReturnValue).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> chainedReturnValue).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            return chainedReturnValue;
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            return chainedReturnValue;
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_whenChained() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            return chainedReturnValue;
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void handle_exceptional() {
        CountDownLatch executed = new CountDownLatch(1);
        CompletableFuture<Object> future = invokeSync(local, true);
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
        CompletableFuture<Object> future = invokeAsync(local, true);
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
        CompletableFuture<Object> future = invokeSync(local, false);
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
        CompletableFuture<Object> future = invokeSync(local, true);
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
        CompletableFuture<Object> future = invokeAsync(local, false);
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
        CompletableFuture<Object> future = invokeAsync(local, true);
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
        CompletableFuture<Object> future = invokeSync(local, false);
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
        CompletableFuture<Object> future = invokeSync(local, true);
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
        CompletableFuture<Object> future = invokeSync(local, true);
        CompletableFuture<Object> chained = future.exceptionally(t -> chainedReturnValue);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void exceptionally_whenAsync() {
        CompletableFuture<Object> future = invokeAsync(local, true);
        CompletableFuture<Object> chained = future.exceptionally(t -> chainedReturnValue);

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void exceptionally_whenExceptionFromExceptionallyFunction() {
        CompletableFuture<Object> future = invokeSync(local, true);
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
        CompletableFuture<Object> future = invokeAsync(local, true);
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
        CompletableFuture<Object> future = invokeSync(local, false);
        CompletableFuture<Object> chained = future.exceptionally(t -> {
            throw new IllegalArgumentException();
        });

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(future.join(), chained.join());
    }

    @Test
    public void thenCompose_onCompletedFuture() {
        CompletableFuture<Object> future = invokeSync(local, false);
        CompletableFuture<Object> composedFuture = future
                .thenCompose(value -> CompletableFuture.completedFuture(returnValue));

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
    }

    @Test
    public void thenCompose_onIncompleteFuture() {
        CompletableFuture<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> composedFuture = future
                .thenCompose(value -> CompletableFuture.completedFuture(returnValue));

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
    }

    @Test
    public void thenComposeAsync_onCompletedFuture() {
        CompletionStage<Object> future = invokeSync(local, false);
        CompletableFuture<Object> composedFuture = future.toCompletableFuture()
                .thenComposeAsync(value -> CompletableFuture.completedFuture(returnValue));

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
    }

    @Test
    public void thenComposeAsync_onIncompleteFuture() {
        CompletableFuture<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> composedFuture = future
                .thenComposeAsync(value -> CompletableFuture.completedFuture(returnValue));

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
    }

    @Test
    public void thenComposeAsync_withExecutor_onCompletedFuture() {
        CompletionStage<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> composedFuture = future.toCompletableFuture()
                .thenComposeAsync(value -> CompletableFuture.completedFuture(returnValue), countingExecutor);

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenComposeAsync_withExecutor_onIncompleteFuture() {
        CompletableFuture<Object> future = invokeAsync(local, false);
        CompletableFuture<Object> composedFuture = future
                .thenComposeAsync(value -> CompletableFuture.completedFuture(returnValue), countingExecutor);

        assertTrueEventually(() -> assertTrue(composedFuture.isDone()));
        assertEquals(returnValue, composedFuture.join());
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test(expected = NullPointerException.class)
    public void thenCompose_whenNullFunction() {
        CompletableFuture<Object> future = invokeSync(local, false);
        future.thenCompose(null);
    }

    @Test(expected = NullPointerException.class)
    public void thenComposeAsync_whenNullFunction() {
        CompletableFuture<Object> future = invokeSync(local, false);
        future.thenComposeAsync(null);
    }

    @Test(expected = NullPointerException.class)
    public void thenComposeAsync_withExecutor_whenNullFunction() {
        CompletableFuture<Object> future = invokeSync(local, false);
        future.thenComposeAsync(null, countingExecutor);
    }

    @Test
    public void thenCompose_whenExceptionFromFirstStage() {
        CompletableFuture<Object> future = invokeSync(local, true);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        future.thenCompose(v -> null).join();
    }

    @Test
    public void thenCompose_whenExceptionFromUserFunction() {
        CompletableFuture<Object> future = invokeSync(local, false);

        expectedException.expect(CompletionException.class);
        expectedException.expectCause(new RootCauseMatcher(IllegalStateException.class));
        future.thenCompose(v -> {
            throw new IllegalStateException();
        }).join();
    }

    @Test
    public void thenCompose_whenExceptionFromFirstStageAndUserFunction_thenFirstStageExceptionBubbles() {
        CompletableFuture<Object> future = invokeSync(local, true);

        expectedException.expect(CompletionException.class);
        // expect the exception thrown from first future
        expectedException.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
        future.thenCompose(v -> {
            throw new IllegalStateException();
        }).join();
    }

    private CompletableFuture<Void> prepareThenAccept(CompletionStage invocationFuture,
                                boolean async,
                                boolean explicitExecutor) {

        CompletionStage chained;
        if (async) {
            if (explicitExecutor) {
                chained = invocationFuture.thenAcceptAsync(value -> {
                    logger.warning(Thread.currentThread() + " >>> " + value);
                }, countingExecutor);
            } else {
                chained = invocationFuture.thenAcceptAsync(value -> {
                    logger.warning(Thread.currentThread() + " >>> " + value);
                });
            }
        } else {
            chained = invocationFuture.thenAccept(value -> {
                logger.warning(Thread.currentThread() + " >>> " + value);
            });
        }
        return chained.toCompletableFuture();
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return config;
    }

    private void ignore() {
    }
}
