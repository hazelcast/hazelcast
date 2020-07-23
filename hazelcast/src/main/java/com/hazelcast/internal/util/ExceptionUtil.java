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
import com.hazelcast.spi.impl.operationservice.WrappableException;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Contains various exception related utility methods.
 */
public final class ExceptionUtil {

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.publicLookup();
    // new Throwable(String message, Throwable cause)
    private static final MethodType MT_INIT_STRING_THROWABLE = MethodType.methodType(void.class, String.class, Throwable.class);
    // new Throwable(Throwable cause)
    private static final MethodType MT_INIT_THROWABLE = MethodType.methodType(void.class, Throwable.class);
    // new Throwable(String message)
    private static final MethodType MT_INIT_STRING = MethodType.methodType(void.class, String.class);

    private static final BiFunction<Throwable, String, HazelcastException> HAZELCAST_EXCEPTION_WRAPPER = (throwable, message) -> {
        if (message != null) {
            return new HazelcastException(message, throwable);
        } else {
            return new HazelcastException(throwable);
        }
    };

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
        return (RuntimeException) peel(t, null, null, HAZELCAST_EXCEPTION_WRAPPER);
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
        return peel(t, allowedType, message, HAZELCAST_EXCEPTION_WRAPPER);
    }

    /**
     * Processes {@code Throwable t} so that the returned {@code Throwable}'s type matches {@code allowedType},
     * {@code RuntimeException} or any {@code Throwable} returned by `exceptionWrapper`
     * Processing may include unwrapping {@code t}'s cause hierarchy, wrapping it in a exception
     * created by using exceptionWrapper or just returning the same instance {@code t}
     * if it is already an instance of {@code RuntimeException}.
     *
     * @param t                {@code Throwable} to be peeled
     * @param allowedType      the type expected to be returned; when {@code null}, this method returns instances
     *                         of {@code RuntimeException} or <W>
     * @param message          if not {@code null}, used as the message in {@code RuntimeException} that
     *                         may wrap the peeled {@code Throwable}
     * @param exceptionWrapper wraps the peeled code using this exceptionWrapper
     * @param <W>              Type of the wrapper exception in exceptionWrapper
     * @param <T>              allowed type of {@code Throwable}
     * @return the peeled {@code Throwable}
     */
    public static <T, W extends Throwable> Throwable peel(final Throwable t, Class<T> allowedType,
                                                          String message, BiFunction<Throwable, String, W> exceptionWrapper) {
        if (t instanceof RuntimeException) {
            return wrapException(t, message, exceptionWrapper);
        }

        if (t instanceof ExecutionException || t instanceof InvocationTargetException) {
            final Throwable cause = t.getCause();
            if (cause != null) {
                return peel(cause, allowedType, message, exceptionWrapper);
            } else {
                return exceptionWrapper.apply(t, message);
            }
        }

        if (allowedType != null && allowedType.isAssignableFrom(t.getClass())) {
            return t;
        }

        return exceptionWrapper.apply(t, message);
    }

    public static <W extends Throwable> Throwable wrapException(Throwable t, String message,
                                                                BiFunction<Throwable, String, W> exceptionWrapper) {
        if (t instanceof WrappableException) {
            return ((WrappableException) t).wrap();
        }
        Throwable wrapped = tryWrapInSameClass(t);
        return wrapped == null ? exceptionWrapper.apply(t, message) : wrapped;
    }

    public static RuntimeException wrapException(RuntimeException t) {
        return (RuntimeException) wrapException(t, null, HAZELCAST_EXCEPTION_WRAPPER);
    }

    public static RuntimeException rethrow(final Throwable t) {
        rethrowIfError(t);
        throw peel(t);
    }

    public static RuntimeException rethrow(Throwable t, BiFunction<Throwable, String, RuntimeException> exceptionWrapper) {
        rethrowIfError(t);
        throw (RuntimeException) peel(t, null, null, exceptionWrapper);
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
            throw wrapError((Error) t);
        }
    }

    public static Error wrapError(Error cause) {
        Error result = tryWrapInSameClass(cause);
        return result == null ? cause : result;
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

    public static <T extends Throwable> T tryWrapInSameClass(T cause) {
        Class<? extends Throwable> exceptionClass = cause.getClass();
        MethodHandle constructor;
        try {
            constructor = LOOKUP.findConstructor(exceptionClass, MT_INIT_STRING_THROWABLE);
            return (T) constructor.invokeWithArguments(cause.getMessage(), cause);
        } catch (Throwable ignored) {
        }
        try {
            constructor = LOOKUP.findConstructor(exceptionClass, MT_INIT_THROWABLE);
            return (T) constructor.invokeWithArguments(cause);
        } catch (Throwable ignored) {
        }
        try {
            constructor = LOOKUP.findConstructor(exceptionClass, MT_INIT_STRING);
            T result = (T) constructor.invokeWithArguments(cause.getMessage());
            result.initCause(cause);
            return result;
        } catch (Throwable ignored) {
        }
        return null;
    }
}
