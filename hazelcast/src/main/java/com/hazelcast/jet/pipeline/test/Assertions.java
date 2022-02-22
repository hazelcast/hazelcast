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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Various assertions which can be used to assert items passing through the
 * pipeline for correctness. Each assertion also returns the stage it is
 * attached to so the assertions could be used in-line.
 * <p>
 * The assertions in this class are to be used together with the {@code apply()}
 * operator on the pipeline. For assertions that can be used directly
 * as sinks, see {@link AssertionSinks}.
 *
 * @since Jet 3.2
 */
public final class Assertions {

    private Assertions() {
    }

    /**
     * Asserts that the previous stage emitted the exact sequence of expected
     * items and nothing else. If the assertion fails, the job will fail with an
     * {@link AssertionError} with the given message.
     * <p>
     * Example:
     * <pre>{@code
     * p.readFrom(TestSources.items(1, 2, 3, 4))
     *  .apply(Assertions.assertOrdered("unexpected values", Arrays.asList(1, 2, 3, 4)))
     *  .writeTo(Sinks.logger());
     * }</pre>
     *
     * <b>Note:</b> Since Jet jobs are distributed, input from multiple upstream
     * processors is merged in a non-deterministic way. Therefore this assertion
     * is recommended only for testing of non-distributed sources.
     */
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertOrdered(
        @Nullable String message,
        @Nonnull Collection<? extends T> expected
    ) {
        return stage -> {
            stage.writeTo(AssertionSinks.assertOrdered(message, expected));
            return stage;
        };
    }

    /**
     * Asserts that the previous stage emitted the exact sequence of expected
     * items and nothing else. If the assertion fails, the job will fail with an
     * {@link AssertionError}.
     * <p>
     * Example:
     * <pre>{@code
     * p.readFrom(TestSources.items(1, 2, 3, 4))
     *  .apply(Assertions.assertOrderedArrays.asList(1, 2, 3, 4)))
     *  .writeTo(Sinks.logger());
     * }</pre>
     *
     * <b>Note:</b> Since Jet jobs are distributed, input from multiple upstream
     * processors is merged in a non-deterministic way. Therefore this assertion
     * is recommended only for testing of non-distributed sources.
     */
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertOrdered(
        @Nonnull Collection<? extends T> expected
    ) {
        return assertOrdered(null, expected);
    }

    /**
     * Asserts that the previous stage emitted the expected items in any order,
     * but nothing else. If the assertion fails, the job will fail with an
     * {@link AssertionError} with the given message.
     * <p>
     * Example:
     * <pre>{@code
     * p.readFrom(TestSources.items(4, 3, 2, 1))
     *  .apply(Assertions.assertAnyOrder("unexpected values", Arrays.asList(1, 2, 3, 4)))
     *  .writeTo(Sinks.logger())
     * }</pre>
     */
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertAnyOrder(
        @Nullable String message,
        @Nonnull Collection<? extends T> expected
    ) {
        return stage -> {
            stage.writeTo(AssertionSinks.assertAnyOrder(message, expected));
            return stage;
        };
    }

    /**
     * Asserts that the previous stage emitted the expected items in any order,
     * but nothing else. If the assertion fails, the job will fail with an
     * {@link AssertionError}.
     * <p>
     * Example:
     * <pre>{@code
     * p.readFrom(TestSources.items(4, 3, 2, 1))
     *  .apply(Assertions.assertAnyOrder(Arrays.asList(1, 2, 3, 4)))
     *  .writeTo(Sinks.logger())
     * }</pre>
     */
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertAnyOrder(
        @Nonnull Collection<? extends T> expected
    ) {
        return assertAnyOrder(null, expected);
    }

    /**
     * Asserts that the previous stage emitted all of the given items in any order.
     * If the assertion fails, the job will fail with an {@link AssertionError} with
     * the given message.
     * <p>
     * Example:
     * <pre>{@code
     * p.readFrom(TestSources.items(4, 3, 2, 1))
     *  .apply(Assertions.assertAnyOrder(Arrays.asList(1, 3)))
     *  .writeTo(Sinks.logger())
     * }</pre>
     */
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertContains(
        @Nullable String message,
        @Nonnull Collection<? extends T> expected
    ) {
        return stage -> {
            stage.writeTo(AssertionSinks.assertContains(message, expected));
            return stage;
        };
    }

    /**
     * Asserts that the previous stage emitted all of the given items in any order.
     * If the assertion fails, the job will fail with a {@link AssertionError} with
     * the given message.
     * <p>
     * Example:
     * <pre>{@code
     * p.readFrom(TestSources.items(4, 3, 2, 1))
     *  .apply(Assertions.assertContains(Arrays.asList(1, 3)))
     *  .writeTo(Sinks.logger())
     * }</pre>
     */
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertContains(
        @Nonnull Collection<? extends T> expected
    ) {
        return assertContains(null, expected);
    }

    /**
     * Collects all the received items in a list and once the upstream stage is
     * completed, it executes the assertion supplied by {@code assertFn}. If no
     * items were collected, it will be called with empty list.
     * <p>
     * Example:
     * <pre>{@code
     * p.readFrom(TestSources.items(1, 2, 3, 4))
     *  .apply(Assertions.assertCollected(items ->
     *          assertTrue("expected minimum of 4 items", items.size() >= 4)))
     *  .writeTo(Sinks.logger())
     * }</pre>
     *
     * <b>Note:</b> This assertion is not usable in streaming jobs. For the streaming
     * equivalent see {@link #assertCollectedEventually}.
     */
    @Nonnull
    public static <T> FunctionEx<BatchStage<T>, BatchStage<T>> assertCollected(
        @Nonnull ConsumerEx<? super List<T>> assertFn
    ) {
        return stage -> {
            stage.writeTo(AssertionSinks.assertCollected(assertFn));
            return stage;
        };
    }

    /**
     * Collects all the received items into a list and runs the {@code assertFn}
     * every time a new item is received. An {@link AssertionError} thrown from
     * the {@code assertFn} will be ignored until {@code timeoutSeconds} have
     * passed, after which the last {@code AssertionError} will be rethrown.
     * If {@code assertFn} throws any other exception, it will be rethrown
     * immediately.
     * <p>
     * When {@code assertFn} completes without any error, the sink will throw
     * an {@link AssertionCompletedException} to indicate success. Exception is
     * used to terminate the job so that you can {@code join()} it.
     * <p>
     * Example:
     * <pre>{@code
     * p.readFrom(TestSources.itemStream(10))
     *  .withoutTimestamps()
     *  .apply(assertCollectedEventually(5, c -> assertTrue("did not receive at least 20 items", c.size() > 20)));
     * }</pre>
     * To use this assertion in a test, you need to catch the thrown exception and validate
     * that it's of the expected type as follows:
     * <pre>{@code
     * try {
     *     jetInstance.newJob(p).join();
     *     Assert.fail("Job should have completed with an AssertionCompletedException, " +
     *             "but completed normally");
     * } catch (CompletionException e) {
     *     String errorMsg = e.getCause().getMessage();
     *     Assert.assertTrue(
     *             "Job was expected to complete with AssertionCompletedException, but completed with: " + e.getCause(),
     *             errorMsg.contains(AssertionCompletedException.class.getName())
     *     );
     * }
     * }</pre>
     *
     * <b>Note:</b> This assertions requires that there are no other assertions in the
     * job as this one can complete the job before the other ones succeeded.
     */
    @Nonnull
    public static <T> FunctionEx<StreamStage<T>, StreamStage<T>> assertCollectedEventually(
        int timeout, @Nonnull ConsumerEx<? super List<T>> assertFn
    ) {
        return stage -> {
            stage.writeTo(AssertionSinks.assertCollectedEventually(timeout, assertFn));
            return stage;
        };
    }
}
