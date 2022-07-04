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

package com.hazelcast.jet.core;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.impl.util.ThrottleWrappedP;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.impl.util.WrappingProcessorMetaSupplier;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertEquals;

public final class TestUtil {

    public static final ExecutorService DIRECT_EXECUTOR = new DirectExecutorService();

    private TestUtil() {
    }

    public static void executeAndPeel(Job job) throws Throwable {
        try {
            job.join();
        } catch (Exception e) {
            throw peel(e);
        }
    }

    /**
     * Asserts that {@code caught} exception is equal to {@code expected} or one of its causes.
     * <p>
     * Exceptions are considered equal, if their {@code message}s and classes are equal OR if the caught exception
     * contains expected message and exception class name (this covers cases where exception is reported as a string
     * from already completed job).
     *
     * @param expected Expected exception
     * @param caught   Caught exception
     */
    public static void assertExceptionInCauses(final Throwable expected, final Throwable caught) {
        // peel until expected error is found
        boolean found = false;
        Throwable t = caught;
        while (!found && t != null) {
            found = Objects.equals(t.getMessage(), expected.getMessage()) && t.getClass() == expected.getClass() ||
                    (expected.getMessage() != null && t.getMessage() != null
                            && t.getMessage().contains(expected.getMessage())
                            && t.getMessage().contains(expected.getClass().getName()));
            t = t.getCause();
        }

        if (!found) {
            StringWriter caughtStr = new StringWriter();
            caught.printStackTrace(new PrintWriter(caughtStr));
            assertEquals("expected exception not found in causes chain", expected.toString(), caughtStr.toString());
        }
    }

    @Nonnull
    public static ProcessorMetaSupplier throttle(@Nonnull SupplierEx<Processor> wrapped, long itemsPerSecond) {
        return new WrappingProcessorMetaSupplier(ProcessorMetaSupplier.of(wrapped), p -> new ThrottleWrappedP(p,
                itemsPerSecond));
    }

    @Nonnull
    public static ProcessorMetaSupplier throttle(@Nonnull ProcessorMetaSupplier wrapped, long itemsPerSecond) {
        return new WrappingProcessorMetaSupplier(wrapped, p -> new ThrottleWrappedP(p, itemsPerSecond));
    }

    /**
     * Create {@code HashSet} from a list of items.
     */
    @Nonnull
    public static <T> Set<T> set(T ... foo) {
        return new HashSet<>(asList(foo));
    }

    /**
     * Create a {@code HashMap} from a list of keys and values. The sequence
     * must have even length and be of the form {@code key1, value1, key2,
     * value2, ...}
     *
     * @throws ArrayIndexOutOfBoundsException if odd number of parameters is
     *      passed
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <K, V> Map<K, V> createMap(Object... keysAndValues) {
        HashMap<K, V> res = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; ) {
            res.put((K) keysAndValues[i++], (V) keysAndValues[i++]);
        }
        return res;
    }

    private static class DirectExecutorService implements ExecutorService {
        private volatile boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Nonnull @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Nonnull @Override
        public <T> Future<T> submit(@Nonnull Callable<T> task) {
            try {
                return completedFuture(task.call());
            } catch (Exception e) {
                return Util.exceptionallyCompletedFuture(e);
            }
        }

        @Nonnull @Override
        public <T> Future<T> submit(@Nonnull Runnable task, T result) {
            return submit(() -> {
                task.run();
                return result;
            });
        }

        @Nonnull @Override
        public Future<?> submit(@Nonnull Runnable task) {
            return submit(task, null);
        }

        @Nonnull @Override
        public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Nonnull @Override
        public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks, long timeout,
                                             @Nonnull TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Nonnull @Override
        public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(@Nonnull Runnable command) {
            submit(command);
        }
    }
}
