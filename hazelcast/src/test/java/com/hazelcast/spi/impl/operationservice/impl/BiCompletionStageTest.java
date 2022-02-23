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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.spi.impl.operationservice.impl.CompletableFutureTestUtil.CountingExecutor;
import com.hazelcast.spi.impl.operationservice.impl.CompletableFutureTestUtil.InvocationPromise;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for InvocationFuture methods operating on two
 * CompletionStages.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BiCompletionStageTest extends HazelcastTestSupport {

    @Parameters(name = "{0},{1}")
    public static Iterable<Object[]> parameters() {
        return asList(
                new Object[][]{
                        // sync - sync
                        {new InvocationPromise(true, false), new InvocationPromise(true, false)},
                        {new InvocationPromise(true, true), new InvocationPromise(true, false)},
                        {new InvocationPromise(true, true), new InvocationPromise(true, true)},
                        {new InvocationPromise(true, false), new InvocationPromise(true, true)},
                        // async - sync
                        {new InvocationPromise(false, false), new InvocationPromise(true, false)},
                        {new InvocationPromise(false, true), new InvocationPromise(true, false)},
                        {new InvocationPromise(false, true), new InvocationPromise(true, true)},
                        {new InvocationPromise(false, false), new InvocationPromise(true, true)},
                        // async - async
                        {new InvocationPromise(false, false), new InvocationPromise(false, false)},
                        {new InvocationPromise(false, true), new InvocationPromise(false, false)},
                        {new InvocationPromise(false, true), new InvocationPromise(false, true)},
                        {new InvocationPromise(false, false), new InvocationPromise(false, true)},
                        // sync - async
                        {new InvocationPromise(true, false), new InvocationPromise(false, false)},
                        {new InvocationPromise(true, true), new InvocationPromise(false, false)},
                        {new InvocationPromise(true, true), new InvocationPromise(false, true)},
                        {new InvocationPromise(true, false), new InvocationPromise(false, true)},
                               });
    }

    @Parameter
    public InvocationPromise invocation1;

    @Parameter(1)
    public InvocationPromise invocation2;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private final Object expectedResult = new Object();

    private HazelcastInstance local;
    private CompletableFuture<Object> future1;
    private CompletableFuture<Object> future2;
    private CountingExecutor countingExecutor;

    @Before
    public void setup() {
        local = createHazelcastInstance(smallInstanceConfig());
        future1 = invocation1.invoke(local);
        future2 = invocation2.invoke(local);
        countingExecutor = new CountingExecutor();
    }

    @Test
    public void thenCombine() {
        CompletableFuture<Integer> combinedFuture = future1.thenCombine(future2, (v1, v2) -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
            assertNull(v1);
            assertNull(v2);
            return 1;
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(combinedFuture.isDone());
        });
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertEquals(1, (int) combinedFuture.join());
    }

    @Test
    public void thenCombineAsync() {
        CompletableFuture<Integer> combinedFuture = future1.thenCombineAsync(future2, (v1, v2) -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
            assertNull(v1);
            assertNull(v2);
            return 1;
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(combinedFuture.isDone());
        });
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertEquals(1, (int) combinedFuture.join());
    }

    @Test
    public void thenCombineAsync_withExecutor() {
        CompletableFuture<Integer> combinedFuture = future1.thenCombineAsync(future2, (v1, v2) -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
            assertNull(v1);
            assertNull(v2);
            return 1;
        }, countingExecutor);
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(combinedFuture.isDone());
        });
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertEquals(1, (int) combinedFuture.join());
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenAcceptBoth() {
        CompletableFuture<Void> combinedFuture = future1.thenAcceptBoth(future2, (v1, v2) -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
            assertNull(v1);
            assertNull(v2);
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(combinedFuture.isDone());
        });
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertNull(combinedFuture.join());
    }

    @Test
    public void thenAcceptBothAsync() {
        CompletableFuture<Void> combinedFuture = future1.thenAcceptBothAsync(future2, (v1, v2) -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
            assertNull(v1);
            assertNull(v2);
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(combinedFuture.isDone());
        });
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertNull(combinedFuture.join());
    }

    @Test
    public void thenAcceptBothAsync_withExecutor() {
        CompletableFuture<Void> combinedFuture = future1.thenAcceptBothAsync(future2, (v1, v2) -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
            assertNull(v1);
            assertNull(v2);
        }, countingExecutor);
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(combinedFuture.isDone());
        });
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertNull(combinedFuture.join());
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void runAfterBoth() {
        CompletableFuture<Void> combinedFuture = future1.runAfterBoth(future2, () -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> assertTrue(combinedFuture.isDone()));
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertNull(combinedFuture.join());
    }

    @Test
    public void runAfterBothAsync() {
        CompletableFuture<Void> combinedFuture = future1.runAfterBothAsync(future2, () -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(combinedFuture.isDone());
        });
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertNull(combinedFuture.join());
    }

    @Test
    public void runAfterBothAsync_withExecutor() {
        CompletableFuture<Void> combinedFuture = future1.runAfterBothAsync(future2, () -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
        }, countingExecutor);
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(combinedFuture.isDone());
        });
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertNull(combinedFuture.join());
        assertEquals(1, countingExecutor.counter.get());
    }


    // {run|accept|apply}AfterEither* tests. Javadoc of CompletionStage states:
    // If a stage is dependent on
    // <em>either</em> of two others, and only one of them completes
    // exceptionally, no guarantees are made about whether the dependent
    // stage completes normally or exceptionally.

    @Test
    public void runAfterEither() {
        CompletableFuture<Void> eitherFuture = future1.runAfterEither(future2, () -> {
            assertTrue(future1.isDone() || future2.isDone());
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(eitherFuture.isDone());
        });
        if (exceptionalCompletion && eitherFuture.isCompletedExceptionally()) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            eitherFuture.join();
        }
        // non-exceptional completion
        assertNull(eitherFuture.join());
    }

    @Test
    public void runAfterEitherAsync() {
        CompletableFuture<Void> eitherFuture = future1.runAfterEitherAsync(future2, () -> {
            assertTrue(future1.isDone() || future2.isDone());
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(eitherFuture.isDone());
        });
        if (exceptionalCompletion && eitherFuture.isCompletedExceptionally()) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            eitherFuture.join();
        }
        // non-exceptional completion
        assertNull(eitherFuture.join());
    }

    @Test
    public void runAfterEitherAsync_withExecutor() {
        AtomicInteger executionCounter = new AtomicInteger();
        CompletableFuture<Void> eitherFuture = future1.runAfterEitherAsync(future2, () -> {
            assertTrue(future1.isDone() || future2.isDone());
            executionCounter.getAndIncrement();
        }, countingExecutor);
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(eitherFuture.isDone());
        });
        if (exceptionalCompletion && eitherFuture.isCompletedExceptionally()) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            eitherFuture.join();
        }
        // non-exceptional completion
        assertNull(eitherFuture.join());
        assertEquals(1, executionCounter.get());
    }

    @Test
    public void acceptEither() {
        CompletableFuture<Void> eitherFuture = future1.acceptEither(future2, value -> {
            assertTrue(future1.isDone() || future2.isDone());
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(eitherFuture.isDone());
        });
        if (exceptionalCompletion && eitherFuture.isCompletedExceptionally()) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            eitherFuture.join();
        }
        // non-exceptional completion
        assertNull(eitherFuture.join());
    }

    @Test
    public void acceptEitherAsync() {
        CompletableFuture<Void> eitherFuture = future1.acceptEitherAsync(future2, value -> {
            assertTrue(future1.isDone() || future2.isDone());
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(eitherFuture.isDone());
        });
        if (exceptionalCompletion && eitherFuture.isCompletedExceptionally()) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            eitherFuture.join();
        }
        // non-exceptional completion
        assertNull(eitherFuture.join());
    }

    @Test
    public void acceptEitherAsync_withExecutor() {
        AtomicInteger executionCounter = new AtomicInteger();
        CompletableFuture<Void> eitherFuture = future1.acceptEitherAsync(future2, value -> {
            assertTrue(future1.isDone() || future2.isDone());
            executionCounter.getAndIncrement();
        }, countingExecutor);
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(eitherFuture.isDone());
        });
        if (exceptionalCompletion && eitherFuture.isCompletedExceptionally()) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            eitherFuture.join();
        }
        // non-exceptional completion
        assertNull(eitherFuture.join());
        assertEquals(1, executionCounter.get());
    }

    @Test
    public void applyToEither() {
        CompletableFuture<Object> eitherFuture = future1.applyToEither(future2, value -> {
            assertTrue(future1.isDone() || future2.isDone());
            return expectedResult;
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(eitherFuture.isDone());
        });
        if (exceptionalCompletion && eitherFuture.isCompletedExceptionally()) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            eitherFuture.join();
        }
        // non-exceptional completion
        assertSame(expectedResult, eitherFuture.join());
    }

    @Test
    public void applyToEitherAsync() {
        CompletableFuture<Object> eitherFuture = future1.applyToEitherAsync(future2, value -> {
            assertTrue(future1.isDone() || future2.isDone());
            return expectedResult;
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(eitherFuture.isDone());
        });
        if (exceptionalCompletion && eitherFuture.isCompletedExceptionally()) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            eitherFuture.join();
        }
        // non-exceptional completion
        assertSame(expectedResult, eitherFuture.join());
    }

    @Test
    public void applyToEitherAsync_withExecutor() {
        AtomicInteger executionCounter = new AtomicInteger();
        CompletableFuture<Object> eitherFuture = future1.applyToEitherAsync(future2, value -> {
            assertTrue(future1.isDone() || future2.isDone());
            executionCounter.getAndIncrement();
            return expectedResult;
        }, countingExecutor);
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(eitherFuture.isDone());
        });
        if (exceptionalCompletion && eitherFuture.isCompletedExceptionally()) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            eitherFuture.join();
        }
        // non-exceptional completion
        assertSame(expectedResult, eitherFuture.join());
        assertEquals(1, executionCounter.get());
    }
}
