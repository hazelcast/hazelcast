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

package com.hazelcast.util;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;

/**
 * Contains various exception related utility methods.
 */
public final class ExceptionUtil {

    private static final String EXCEPTION_SEPARATOR = "------ submitted from ------";
    private static final String EXCEPTION_MESSAGE_SEPARATOR = "------ %MSG% ------";
    private static final ExceptionWrapper<RuntimeException> HAZELCAST_EXCEPTION_WRAPPER =
            new ExceptionWrapper<RuntimeException>() {
                @Override
                public RuntimeException create(Throwable throwable, String message) {
                    if (message != null) {
                        return new HazelcastException(message, throwable);
                    } else {
                        return new HazelcastException(throwable);
                    }
                }
            };

    /**
     * Interface used by rethrow/peel to wrap the peeled exception
     */
    public interface ExceptionWrapper<T extends Throwable> {
        T create(Throwable throwable, String message);
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
                                                          String message, ExceptionWrapper<W> exceptionWrapper) {
        if (t instanceof RuntimeException) {
            return t;
        }

        if (t instanceof ExecutionException || t instanceof InvocationTargetException) {
            final Throwable cause = t.getCause();
            if (cause != null) {
                return peel(cause, allowedType, message, exceptionWrapper);
            } else {
                return exceptionWrapper.create(t, message);
            }
        }

        if (allowedType != null && allowedType.isAssignableFrom(t.getClass())) {
            return t;
        }

        return exceptionWrapper.create(t, message);
    }

    public static RuntimeException rethrow(final Throwable t) {
        rethrowIfError(t);
        throw peel(t);
    }

    public static RuntimeException rethrow(final Throwable t, ExceptionWrapper<RuntimeException> exceptionWrapper) {
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

    private static void rethrowIfError(final Throwable t) {
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
    public static <T> T sneakyThrow(Throwable t) {
        ExceptionUtil.<RuntimeException>sneakyThrowInternal(t);
        return (T) t;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrowInternal(Throwable t) throws T {
        throw (T) t;
    }

    /**
     * This method changes the given async cause, and it adds the also given local stacktrace.<br/>
     * If the remoteCause is an {@link java.util.concurrent.ExecutionException} and it has a non-null inner
     * cause, this inner cause is unwrapped and the local stacktrace and exception message are added to the
     * that instead of the given asyncCause itself.
     *
     * @param asyncCause          the async exception
     * @param localSideStackTrace the local stacktrace to add to the exception stacktrace
     */
    public static void fixAsyncStackTrace(Throwable asyncCause, StackTraceElement[] localSideStackTrace) {
        Throwable throwable = asyncCause;
        if (asyncCause instanceof ExecutionException && throwable.getCause() != null) {
            throwable = throwable.getCause();
        }

        StackTraceElement[] remoteStackTrace = throwable.getStackTrace();
        StackTraceElement[] newStackTrace = new StackTraceElement[localSideStackTrace.length + remoteStackTrace.length];
        System.arraycopy(remoteStackTrace, 0, newStackTrace, 0, remoteStackTrace.length);
        newStackTrace[remoteStackTrace.length] = new StackTraceElement(EXCEPTION_SEPARATOR, "", null, -1);
        System.arraycopy(localSideStackTrace, 1, newStackTrace, remoteStackTrace.length + 1, localSideStackTrace.length - 1);
        throwable.setStackTrace(newStackTrace);
    }

    /**
     * This method changes the given async cause, and it adds the also given local stacktrace separated by the
     * supplied exception message.<br/>
     * If the remoteCause is an {@link java.util.concurrent.ExecutionException} and it has a non-null inner
     * cause, this inner cause is unwrapped and the local stacktrace and exception message are added to the
     * that instead of the given remoteCause itself.
     *
     * @param asyncCause            the async exception
     * @param localSideStackTrace   the local stacktrace to add to the exceptions stacktrace
     * @param localExceptionMessage a special exception message which is added to the stacktrace
     */
    public static void fixAsyncStackTrace(Throwable asyncCause, StackTraceElement[] localSideStackTrace,
                                          String localExceptionMessage) {
        Throwable throwable = asyncCause;
        if (asyncCause instanceof ExecutionException && throwable.getCause() != null) {
            throwable = throwable.getCause();
        }

        String msg = EXCEPTION_MESSAGE_SEPARATOR.replace("%MSG%", localExceptionMessage);
        StackTraceElement[] remoteStackTrace = throwable.getStackTrace();
        StackTraceElement[] newStackTrace = new StackTraceElement[localSideStackTrace.length + remoteStackTrace.length + 1];
        System.arraycopy(remoteStackTrace, 0, newStackTrace, 0, remoteStackTrace.length);
        newStackTrace[remoteStackTrace.length] = new StackTraceElement(EXCEPTION_SEPARATOR, "", null, -1);
        StackTraceElement nextElement = localSideStackTrace[1];
        newStackTrace[remoteStackTrace.length + 1] = new StackTraceElement(msg, nextElement.getMethodName(),
                nextElement.getFileName(), nextElement.getLineNumber());
        System.arraycopy(localSideStackTrace, 1, newStackTrace, remoteStackTrace.length + 2, localSideStackTrace.length - 1);
        throwable.setStackTrace(newStackTrace);
    }

    /**
     * Tries to create the exception with appropriate constructor in the following order.
     * In all cases the cause is set(via constructor or via initCause)
     * new Throwable(String message, Throwable cause)
     * new Throwable(Throwable cause)
     * new Throwable(String message)
     * new Throwable()
     *
     * @param exceptionClass class of the exception
     * @param message        message to be pass to constructor of the exception
     * @param cause          cause to be set to the exception
     * @return null if can not find a constructor as described above, otherwise return newly constructed expcetion
     */
    public static <T extends Throwable> T tryCreateExceptionWithMessageAndCause(Class<? extends Throwable> exceptionClass,
                                                                                String message, @Nullable Throwable cause) {

        try {
            Constructor<? extends Throwable> constructor = exceptionClass.getConstructor(String.class, Throwable.class);
            T clone = (T) constructor.newInstance(message, cause);
            return clone;
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
        }
        try {
            Constructor<? extends Throwable> constructor = exceptionClass.getConstructor(Throwable.class);
            T clone = (T) constructor.newInstance(cause);
            return clone;
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
        }
        try {
            Constructor<? extends Throwable> constructor = exceptionClass.getConstructor(String.class);
            T clone = (T) constructor.newInstance(message);
            clone.initCause(cause);
            return clone;
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
        }
        try {
            Constructor<? extends Throwable> constructor = exceptionClass.getConstructor();
            T clone = (T) constructor.newInstance();
            clone.initCause(cause);
            return clone;
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
        }
        return null;
    }
}
