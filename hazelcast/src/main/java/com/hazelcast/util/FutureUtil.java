/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.transaction.TransactionTimedOutException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

/**
 * This utility class contains convenience methods to work with multiple
 * futures at the same time, e.g.
 * {@link #waitWithDeadline
 */
public final class FutureUtil {

    /**
     * Just rethrows <b>all</b> exceptions
     */
    public static final ExceptionHandler RETHROW_EVERYTHING = new ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    };

    /**
     * Ignores <b>all</b> exceptions
     */
    public static final ExceptionHandler IGNORE_ALL_EXCEPTIONS = new ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
        }
    };

    private static final class CollectAllExceptionHandler implements ExceptionHandler {

        private List<Throwable> throwables;

        private CollectAllExceptionHandler(int count) {
            this.throwables = Collections.synchronizedList(new ArrayList<Throwable>(count));
        }

        @Override
        public void handleException(Throwable throwable) {
            throwables.add(throwable);
        }

        public List<Throwable> getThrowables() {
            return throwables;
        }
    }

    /**
     * Handler for transaction specific rethrown of exceptions.
     */
    public static final ExceptionHandler RETHROW_TRANSACTION_EXCEPTION = new ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            if (throwable instanceof TimeoutException) {
                throw new TransactionTimedOutException(throwable);
            }
            throw ExceptionUtil.rethrow(throwable);
        }
    };

    private FutureUtil() {
    }

    /**
     * This ExceptionHandler rethrows {@link java.util.concurrent.ExecutionException}s and logs
     * {@link com.hazelcast.core.MemberLeftException}s to the log.
     *
     * @param logger  the ILogger instance to be used for logging
     * @param message the log message to appear in the logs before the stacktrace
     * @param level   the log level to be used for logging
     */
    @PrivateApi
    public static ExceptionHandler logAllExceptions(final ILogger logger, final String message, final Level level) {
        if (logger.isLoggable(level)) {
            return new ExceptionHandler() {
                @Override
                public void handleException(Throwable throwable) {
                    logger.log(level, message, throwable);
                }
            };
        }
        return IGNORE_ALL_EXCEPTIONS;
    }

    /**
     * This ExceptionHandler rethrows {@link java.util.concurrent.ExecutionException}s and logs
     * {@link com.hazelcast.core.MemberLeftException}s to the log.
     *
     * @param logger the ILogger instance to be used for logging
     * @param level  the log level to be used for logging
     */
    @PrivateApi
    public static ExceptionHandler logAllExceptions(final ILogger logger, final Level level) {
        if (logger.isLoggable(level)) {
            return new ExceptionHandler() {
                @Override
                public void handleException(Throwable throwable) {
                    logger.log(level, "Exception occurred", throwable);
                }
            };
        }
        return IGNORE_ALL_EXCEPTIONS;
    }

    @PrivateApi
    public static <V> Collection<V> returnWithDeadline(Collection<Future<V>> futures, long timeout, TimeUnit timeUnit,
                                                       ExceptionHandler exceptionHandler) {

        return returnWithDeadline(futures, timeout, timeUnit, timeout, timeUnit, exceptionHandler);
    }

    @PrivateApi
    public static <V> Collection<V> returnWithDeadline(Collection<Future<V>> futures,
                                                       long overallTimeout, TimeUnit overallTimeUnit,
                                                       long perFutureTimeout, TimeUnit perFutureTimeUnit,
                                                       ExceptionHandler exceptionHandler) {

        // Calculate timeouts for whole operation and per future. If corresponding TimeUnits not set assume
        // the default of TimeUnit.SECONDS
        long overallTimeoutNanos = calculateTimeout(overallTimeout, overallTimeUnit);
        long perFutureTimeoutNanos = calculateTimeout(perFutureTimeout, perFutureTimeUnit);

        // Common deadline for all futures
        long deadline = System.nanoTime() + overallTimeoutNanos;

        List<V> results = new ArrayList<V>(futures.size());
        for (Future<V> future : futures) {
            try {
                long timeoutNanos = calculateFutureTimeout(perFutureTimeoutNanos, deadline);
                V value = executeWithDeadline(future, timeoutNanos);
                if (value != null) {
                    results.add(value);
                }
            } catch (Exception e) {
                exceptionHandler.handleException(e);
            }
        }
        return results;
    }

    @PrivateApi
    public static void waitWithDeadline(Collection<Future> futures, long timeout, TimeUnit timeUnit, final ILogger logger) {
        waitWithDeadline(futures, timeout, timeUnit, new ExceptionHandler() {
            @Override
            public void handleException(Throwable throwable) {
                if (throwable instanceof MemberLeftException) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Member left while waiting for futures...", throwable);
                    }
                }
            }
        });
    }

    @PrivateApi
    public static void waitUntilAllRespondedWithDeadline(Collection<Future> futures, long timeout, TimeUnit timeUnit,
                                                         ExceptionHandler exceptionHandler) {
        CollectAllExceptionHandler collector = new CollectAllExceptionHandler(futures.size());
        waitWithDeadline(futures, timeout, timeUnit, collector);
        final List<Throwable> throwables = collector.getThrowables();
        // synchronized list does not provide thread-safety guarantee for iteration so we handle it ourselves.
        synchronized (throwables) {
            for (Throwable t : throwables) {
                exceptionHandler.handleException(t);
            }
        }
    }

    @PrivateApi
    public static void waitWithDeadline(Collection<Future> futures, long timeout, TimeUnit timeUnit,
                                        ExceptionHandler exceptionHandler) {

        waitWithDeadline(futures, timeout, timeUnit, timeout, timeUnit, exceptionHandler);
    }

    @PrivateApi
    public static void waitWithDeadline(Collection<Future> futures, long overallTimeout, TimeUnit overallTimeUnit,
                                        long perFutureTimeout, TimeUnit perFutureTimeUnit, ExceptionHandler exceptionHandler) {

        // Calculate timeouts for whole operation and per future. If corresponding TimeUnits not set assume
        // the default of TimeUnit.SECONDS
        long overallTimeoutNanos = calculateTimeout(overallTimeout, overallTimeUnit);
        long perFutureTimeoutNanos = calculateTimeout(perFutureTimeout, perFutureTimeUnit);

        // Common deadline for all futures
        long deadline = System.nanoTime() + overallTimeoutNanos;

        for (Future future : futures) {
            try {
                long timeoutNanos = calculateFutureTimeout(perFutureTimeoutNanos, deadline);
                executeWithDeadline(future, timeoutNanos);
            } catch (Throwable e) {
                exceptionHandler.handleException(e);
            }
        }
    }

    private static <V> V executeWithDeadline(Future<V> future, long timeoutNanos) throws Exception {
        if (timeoutNanos <= 0) {
            // Maybe we just finished in time
            if (future.isDone() || future.isCancelled()) {
                return retrieveValue(future);
            } else {
                throw new TimeoutException();
            }
        }
        return future.get(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private static <V> V retrieveValue(Future<V> future)
            throws ExecutionException, InterruptedException {

        if (future instanceof InternalCompletableFuture) {
            return ((InternalCompletableFuture<V>) future).join();
        }

        return future.get();
    }

    private static long calculateTimeout(long timeout, TimeUnit timeUnit) {
        timeUnit = timeUnit == null ? TimeUnit.SECONDS : timeUnit;
        return timeUnit.toNanos(timeout);
    }

    private static long calculateFutureTimeout(long perFutureTimeoutNanos, long deadline) {
        long remainingNanos = deadline - System.nanoTime();
        return Math.min(remainingNanos, perFutureTimeoutNanos);
    }

    /**
     * Internally used interface to define behavior of the FutureUtil methods when exceptions arise
     */
    public interface ExceptionHandler {
        void handleException(Throwable throwable);
    }

    /**
     * Check if all futures are done
     *
     * @param futures
     * @return true if all futures are done. false otherwise
     */
    public static boolean allDone(Collection<Future> futures) {
        for (Future f : futures) {
            if (!f.isDone()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Rethrow exeception of the fist future that completed with an exception
     *
     * @param futures
     * @throws Exception
     */
    public static void checkAllDone(Collection<Future> futures) throws Exception {
        for (Future f : futures) {
            if (f.isDone()) {
                f.get();
            }
        }
    }

    /**
     * Get all futures that are done
     *
     * @param futures
     * @return list of completed futures
     */
    public static List<Future> getAllDone(Collection<Future> futures) {
        List<Future> doneFutures = new ArrayList<Future>();
        for (Future f : futures) {
            if (f.isDone()) {
                doneFutures.add(f);
            }
        }
        return doneFutures;
    }
}
