/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.transaction.TransactionTimedOutException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

/**
 * This utility class contains convenience methods to work with multiple
 * futures at the same time, e.g.
 * {@link #waitWithDeadline(java.util.Collection, long, java.util.concurrent.TimeUnit, long, java.util.concurrent.TimeUnit)}
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

    /**
     * Ignores all exceptions but still logs {@link com.hazelcast.core.MemberLeftException} per future and just tries
     * to finish all of the given ones. This is the default behavior if nothing else is given.
     */
    public static final ExceptionHandler IGNORE_ALL_EXCEPT_LOG_MEMBER_LEFT = new ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            if (throwable instanceof MemberLeftException) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Member left while waiting for futures...", throwable);
                }
            }
        }
    };

    /**
     * This ExceptionHandler rethrows {@link java.util.concurrent.ExecutionException}s and logs
     * {@link com.hazelcast.core.MemberLeftException}s to the log.
     */
    public static final ExceptionHandler RETHROW_EXECUTION_EXCEPTION = new ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            if (throwable instanceof MemberLeftException) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Member left while waiting for futures...", throwable);
                }
            } else if (throwable instanceof ExecutionException) {
                throw new HazelcastException(throwable);
            }
        }
    };


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

    private static final ILogger LOGGER = Logger.getLogger(FutureUtil.class);

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
     * @param message the log message to appear in the logs before the stacktrace
     * @param level   the log level to be used for logging
     */
    @PrivateApi
    public static ExceptionHandler logAllExceptions(final String message, final Level level) {
        if (LOGGER.isLoggable(level)) {
            return new ExceptionHandler() {
                @Override
                public void handleException(Throwable throwable) {
                    LOGGER.log(level, message, throwable);
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

    /**
     * This ExceptionHandler rethrows {@link java.util.concurrent.ExecutionException}s and logs
     * {@link com.hazelcast.core.MemberLeftException}s to the log.
     *
     * @param level the log level to be used for logging
     */
    @PrivateApi
    public static ExceptionHandler logAllExceptions(final Level level) {
        if (LOGGER.isLoggable(level)) {
            return new ExceptionHandler() {
                @Override
                public void handleException(Throwable throwable) {
                    LOGGER.log(level, "Exception occurred", throwable);
                }
            };
        }
        return IGNORE_ALL_EXCEPTIONS;
    }

    @PrivateApi
    public static <V> Collection<V> returnWithDeadline(Collection<Future<V>> futures, long timeout, TimeUnit timeUnit) {
        return returnWithDeadline(futures, timeout, timeUnit, IGNORE_ALL_EXCEPT_LOG_MEMBER_LEFT);
    }

    @PrivateApi
    public static <V> Collection<V> returnWithDeadline(Collection<Future<V>> futures, long timeout, TimeUnit timeUnit,
                                                       ExceptionHandler exceptionHandler) {

        return returnWithDeadline(futures, timeout, timeUnit, timeout, timeUnit, exceptionHandler);
    }

    @PrivateApi
    public static <V> Collection<V> returnWithDeadline(Collection<Future<V>> futures,
                                                       long overallTimeout, TimeUnit overallTimeUnit,
                                                       long perFutureTimeout, TimeUnit perFutureTimeUnit) {

        return returnWithDeadline(futures, overallTimeout, overallTimeUnit, perFutureTimeout, perFutureTimeUnit,
                IGNORE_ALL_EXCEPT_LOG_MEMBER_LEFT);
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
    public static void waitWithDeadline(Collection<Future> futures, long timeout, TimeUnit timeUnit) {
        waitWithDeadline(futures, timeout, timeUnit, IGNORE_ALL_EXCEPT_LOG_MEMBER_LEFT);
    }

    @PrivateApi
    public static void waitWithDeadline(Collection<Future> futures, long timeout, TimeUnit timeUnit,
                                        ExceptionHandler exceptionHandler) {

        waitWithDeadline(futures, timeout, timeUnit, timeout, timeUnit, exceptionHandler);
    }

    @PrivateApi
    public static void waitWithDeadline(Collection<Future> futures, long overallTimeout, TimeUnit overallTimeUnit,
                                        long perFutureTimeout, TimeUnit perFutureTimeUnit) {

        waitWithDeadline(futures, overallTimeout, overallTimeUnit, perFutureTimeout, perFutureTimeUnit,
                IGNORE_ALL_EXCEPT_LOG_MEMBER_LEFT);
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
            return ((InternalCompletableFuture<V>) future).getSafely();
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
}
