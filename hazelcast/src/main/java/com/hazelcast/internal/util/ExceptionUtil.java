/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * Contains various exception related utility methods.
 */
public final class ExceptionUtil {

    private static final RuntimeExceptionFactory HAZELCAST_EXCEPTION_FACTORY = (throwable, message) -> {
        if (message != null) {
            return new HazelcastException(message, throwable);
        } else {
            return new HazelcastException(throwable);
        }
    };

    /**
     * Interface used by rethrow/peel to wrap the peeled exception
     */
    public interface RuntimeExceptionFactory {
        RuntimeException create(Throwable throwable, String message);
    }

    private ExceptionUtil() {
    }

    /**
     * Converts a Throwable stacktrace to a String.
     *
     * @param cause the Throwable
     * @return the String.
     */
    public static String toString(Throwable cause) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        cause.printStackTrace(pw);
        return sw.toString();
    }

    public static RuntimeException peel(final Throwable t) {
        return (RuntimeException) peel(t, null, null, HAZELCAST_EXCEPTION_FACTORY);
    }

    /**
     * Processes {@code Throwable t} so that the returned {@code Throwable}'s type matches {@code allowedType} or
     * {@code RuntimeException}. Processing may include unwrapping {@code t}'s cause hierarchy, wrapping it in a
     * {@code HazelcastException} or just returning the same instance {@code t} if it is already an instance of
     * {@code RuntimeException}.
     *
     * @param t           {@code Throwable} to be peeled
     * @param allowedType the type expected to be returned; when {@code null}, this method returns instances
     *                    of {@code RuntimeException}
     * @param message     if not {@code null}, used as the message in the {@code HazelcastException} that
     *                    may wrap the peeled {@code Throwable}
     * @param <T>         expected type of {@code Throwable}
     * @return the peeled {@code Throwable}
     */
    public static <T extends Throwable> Throwable peel(final Throwable t, Class<T> allowedType, String message) {
        return peel(t, allowedType, message, HAZELCAST_EXCEPTION_FACTORY);
    }

    /**
     * Processes {@code Throwable t} so that the returned {@code Throwable}'s type matches {@code allowedType} or
     * {@code RuntimeException}. Processing may include unwrapping {@code t}'s cause hierarchy, wrapping it in a
     * {@code RuntimeException} created by using runtimeExceptionFactory or just returning the same instance {@code t}
     * if it is already an instance of {@code RuntimeException}.
     *
     * @param t                       {@code Throwable} to be peeled
     * @param allowedType             the type expected to be returned; when {@code null}, this method returns instances
     *                                of {@code RuntimeException}
     * @param message                 if not {@code null}, used as the message in {@code RuntimeException} that
     *                                may wrap the peeled {@code Throwable}
     * @param runtimeExceptionFactory wraps the peeled code using this runtimeExceptionFactory
     * @param <T>                     expected type of {@code Throwable}
     * @return the peeled {@code Throwable}
     */
    public static <T extends Throwable> Throwable peel(final Throwable t, Class<T> allowedType,
                                                       String message, RuntimeExceptionFactory runtimeExceptionFactory) {
        if (t instanceof RuntimeException) {
            return t;
        }

        if (t instanceof ExecutionException || t instanceof InvocationTargetException) {
            final Throwable cause = t.getCause();
            if (cause != null) {
                return peel(cause, allowedType, message, runtimeExceptionFactory);
            } else {
                return runtimeExceptionFactory.create(t, message);
            }
        }

        if (allowedType != null && allowedType.isAssignableFrom(t.getClass())) {
            return t;
        }

        return runtimeExceptionFactory.create(t, message);
    }

    public static RuntimeException rethrow(final Throwable t) {
        rethrowIfError(t);
        throw peel(t);
    }

    public static RuntimeException rethrow(final Throwable t, RuntimeExceptionFactory runtimeExceptionFactory) {
        rethrowIfError(t);
        throw (RuntimeException) peel(t, null, null, runtimeExceptionFactory);
    }

    public static <T extends Throwable> RuntimeException rethrow(final Throwable t, Class<T> allowedType) throws T {
        rethrowIfError(t);
        throw (T) peel(t, allowedType, null);
    }

    /**
     * This rethrow the exception providing an allowed Exception in first priority, even it is a Runtime exception
     */
    public static <T extends Throwable> RuntimeException rethrowAllowedTypeFirst(final Throwable t,
                                                                                 Class<T> allowedType) throws T {
        rethrowIfError(t);
        if (allowedType.isAssignableFrom(t.getClass())) {
            throw (T) t;
        } else {
            throw peel(t);
        }
    }

    public static void rethrowIfError(final Throwable t) {
        if (t instanceof Error) {
            if (t instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) t);
            }
            throw (Error) t;
        }
    }

    public static RuntimeException rethrowAllowInterrupted(final Throwable t) throws InterruptedException {
        return rethrow(t, InterruptedException.class);
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T extends Throwable> RuntimeException sneakyThrow(@Nonnull Throwable t) throws T {
        throw (T) t;
    }

    /**
     * Utility to make sure exceptions inside
     * {@link java.util.concurrent.CompletionStage#whenComplete(BiConsumer)} are not swallowed.
     * Exceptions will be caught and logged using the supplied logger.
     */
    @Nonnull
    public static <T> BiConsumer<T, ? super Throwable> withTryCatch(
            @Nonnull ILogger logger, @Nonnull BiConsumer<T, ? super Throwable> consumer
    ) {
        return withTryCatch(logger, "Exception during callback", consumer);
    }

    /**
     * Utility to make sure exceptions inside
     * {@link java.util.concurrent.CompletionStage#whenComplete(BiConsumer)} are not swallowed.
     * Exceptions will be caught and logged using the supplied logger and message.
     */
    @Nonnull
    public static <T> BiConsumer<T, ? super Throwable> withTryCatch(
            @Nonnull ILogger logger, @Nonnull String message, @Nonnull BiConsumer<T, ? super Throwable> consumer
    ) {
        return (r, t) -> {
            try {
                consumer.accept(r, t);
            } catch (Throwable e) {
                logger.severe(message, e);
            }
        };
    }
}
