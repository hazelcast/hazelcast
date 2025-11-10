/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.pipeline.Sink;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.test.JetAssert.assertEquals;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Various assertions which can be used to assert items on the output of a
 * pipeline.
 * <p>
 * In this class there are variants that can be used as sinks in the pipeline.
 * Variants that can be used in-line in a pipeline are in {@link Assertions}.
 *
 * @since Jet 3.2
 */
public final class AssertionSinks {

    private AssertionSinks() {
    }

    /**
     * Asserts that the previous stage emitted the exact sequence of expected
     * items and nothing else. If the assertion fails, the job will fail with an
     * {@link AssertionError} with the given message.
     * <p>
     * Since Jet jobs are distributed, input from multiple upstream processors
     * is merged in a non-deterministic way. Therefore, this assertion is usable
     * only for testing of non-distributed sources.
     */
    @Nonnull
    public static <T> Sink<T> assertOrdered(@Nullable String message, @Nonnull Collection<? extends T> expected) {
        final List<? super T> exp = new ArrayList<>(expected);
        return assertCollected(received -> assertEquals(message, exp, received));
    }

    /**
     * Asserts that the previous stage emitted the exact sequence of expected
     * items and nothing else. If the assertion fails, the job will fail with an
     * {@link AssertionError}.
     * <p>
     * Since Jet jobs are distributed, input from multiple upstream processors
     * is merged in a non-deterministic way. Therefore, this assertion is usable
     * only for testing of non-distributed sources.
     */
    @Nonnull
    public static <T> Sink<T> assertOrdered(@Nonnull Collection<? extends T> expected) {
        return assertOrdered(null, expected);
    }

    /**
     * Asserts that the previous stage emitted the expected items in any order,
     * but nothing else. If the assertion fails, the job will fail with an
     * {@link AssertionError} with the given message.
     */
    @Nonnull
    public static <T> Sink<T> assertAnyOrder(@Nullable String message, @Nonnull Collection<? extends T> expected) {
        Map<? extends T, Long> expBag = toBag(expected);
        return assertCollected(received -> {
            String msg = "Expected and received did not match. The items are printed in the format of a map as follows:" +
                " {<item>=<num occurrences>}";
            assertEquals(message == null ? msg : message + ", " + msg, expBag, toBag(received));
        });
    }

    /**
     * Asserts that the previous stage emitted the expected items in any order,
     * but nothing else. If the assertion fails, the job will fail with an
     * {@link AssertionError}.
     */
    @Nonnull
    public static <T> Sink<T> assertAnyOrder(@Nonnull Collection<? extends T> expected) {
        return assertAnyOrder(null, expected);
    }

    private static <T> Map<T, Long> toBag(Collection<T> coll) {
        return coll.stream().collect(Collectors.groupingBy(c -> c, Collectors.counting()));
    }

    /**
     * Asserts that the previous stage emitted all the given items in any order.
     * If the assertion fails, the job will fail with a {@link AssertionError} with
     * the given message.
     */
    @Nonnull
    public static <T> Sink<T> assertContains(@Nullable String message, @Nonnull Collection<? extends T> expected) {
        final HashSet<? super T> set = new HashSet<>(expected);
        return AssertionSinkBuilder.assertionSink("assertContains", () -> set)
            .<T>receiveFn(HashSet::remove)
            .completeFn(exp -> assertTrue(
                    message + ", the following items have not been observed: " + exp,
                    exp.isEmpty()))
            .build();
    }

    /**
     * Collects all the received items in a list and once the upstream stage is
     * completed it executes the assertion supplied by {@code assertFn}. If no
     * items were collected, it will be called with empty list.
     * <p>
     * Not usable in streaming jobs - use {@link #assertCollectedEventually}.
     *
     * @param assertFn assertion to execute once all items are received
     */
    @Nonnull
    public static <T> Sink<T> assertCollected(@Nonnull ConsumerEx<? super List<T>> assertFn) {
        return AssertionSinkBuilder.assertionSink("assertCollected", ArrayList<T>::new)
            .<T>receiveFn(ArrayList::add)
            .completeFn(assertFn)
            .build();
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
     * used to terminate the job so that you can {@code join()} it. This also
     * requires that there are no other assertions in the job as this one can
     * complete the job before the other ones succeeded.
     * <p>
     * The assertion can be validated as follows:
     * <pre>
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
     * }</pre>
     *
     * @param timeoutSeconds timeout in seconds, after which any assertion error will be propagated
     * @param assertFn assertion to execute periodically
     */
    @Nonnull
    public static <T> Sink<T> assertCollectedEventually(
            int timeoutSeconds,
            @Nonnull ConsumerEx<? super List<T>> assertFn
    ) {
        return AssertionSinkBuilder
                .assertionSink("assertCollectedEventually",
                        () -> new CollectingSinkWithTimer<>(assertFn, timeoutSeconds))
            .<T>receiveFn(CollectingSinkWithTimer::receive)
            .timerFn(CollectingSinkWithTimer::timer)
            .completeFn(CollectingSinkWithTimer::complete)
            .build();
    }

    private static final class CollectingSinkWithTimer<T> {

        private final long start = System.nanoTime();
        private final List<T> collected = new ArrayList<>();

        private final ConsumerEx<? super List<T>> assertFn;
        private final long timeoutNanos;
        private AssertionError lastError;

        CollectingSinkWithTimer(ConsumerEx<? super List<T>> assertFn, int timeoutSeconds) {
            this.assertFn = assertFn;
            this.timeoutNanos = SECONDS.toNanos(timeoutSeconds);
        }

        void receive(T item) {
            collected.add(item);
            try {
                assertFn.accept(collected);
                throw new AssertionCompletedException("Assertion passed successfully");
            } catch (AssertionError e) {
                lastError = e;
            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        void timer() {
            if (System.nanoTime() - start > timeoutNanos) {
                throw new AssertionError(lastError);
            }
        }

        void complete() {
            assertFn.accept(collected);
        }
    }
}
