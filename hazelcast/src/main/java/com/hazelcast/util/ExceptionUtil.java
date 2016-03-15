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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ExecutionException;

/**
 * Contains various exception related utility methods.
 */
public final class ExceptionUtil {

    private static final String EXCEPTION_SEPARATOR = "------ submitted from ------";
    private static final String EXCEPTION_MESSAGE_SEPARATOR = "------ %MSG% ------";

    //we don't want instances
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
        return (RuntimeException) peel(t, null);
    }

    public static <T extends Throwable> Throwable peel(final Throwable t, Class<T> allowedType) {
        if (t instanceof RuntimeException) {
            return t;
        }

        if (t instanceof ExecutionException) {
            final Throwable cause = t.getCause();
            if (cause != null) {
                return peel(cause, allowedType);
            } else {
                return new HazelcastException(t);
            }
        }

        if (allowedType != null && allowedType.isAssignableFrom(t.getClass())) {
            return t;
        }

        return new HazelcastException(t);
    }

    public static RuntimeException rethrow(final Throwable t) {
        if (t instanceof Error) {
            if (t instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) t);
            }
            throw (Error) t;
        } else {
            throw peel(t);
        }
    }

    public static <T extends Throwable> RuntimeException rethrow(final Throwable t, Class<T> allowedType) throws T {
        if (t instanceof Error) {
            if (t instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) t);
            }
            throw (Error) t;
        } else {
            throw (T) peel(t, allowedType);
        }
    }

    /**
     * This rethrow the exception providing an allowed Exception in first priority, even it is a Runtime exception
     */
    public static <T extends Throwable> RuntimeException rethrowAllowedTypeFirst(final Throwable t,
                                                                                 Class<T> allowedType) throws T {
        if (t instanceof Error) {
            if (t instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) t);
            }
            throw (Error) t;
        } else if (allowedType.isAssignableFrom(t.getClass())) {
            throw (T) t;
        } else {
            throw peel(t);
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
     * @param asyncCause         the async exception
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
     * @param asyncCause           the async exception
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


}
