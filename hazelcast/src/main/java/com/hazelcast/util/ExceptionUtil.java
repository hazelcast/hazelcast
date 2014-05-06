/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.concurrent.ExecutionException;

/**
 * @author mdogan 2/11/13
 */
public final class ExceptionUtil {

    private static final String EXCEPTION_SEPARATOR = "------ End remote and begin local stack-trace ------";
    private static final String EXCEPTION_MESSAGE_SEPARATOR = "------ %MSG% ------";

    //we don't want instances
    private ExceptionUtil() {
    }

    public static RuntimeException rethrow(final Throwable t) {
        if (t instanceof Error) {
            if (t instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) t);
            }
            throw (Error) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof ExecutionException) {
            final Throwable cause = t.getCause();
            if (cause != null) {
                throw rethrow(cause);
            } else {
                throw new HazelcastException(t);
            }
        } else {
            throw new HazelcastException(t);
        }
    }

    public static <T extends Throwable> RuntimeException rethrow(final Throwable t, Class<T> allowedType) throws T {
        if (t instanceof Error) {
            if (t instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) t);
            }
            throw (Error) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof ExecutionException) {
            final Throwable cause = t.getCause();
            if (cause != null) {
                throw rethrow(cause, allowedType);
            } else {
                throw new HazelcastException(t);
            }
        } else if (allowedType.isAssignableFrom(t.getClass())) {
            throw (T) t;
        } else {
            throw new HazelcastException(t);
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


    public static void fixRemoteStackTrace(Throwable remoteCause, StackTraceElement[] localSideStackTrace) {
        StackTraceElement[] remoteStackTrace = remoteCause.getStackTrace();
        StackTraceElement[] newStackTrace = new StackTraceElement[localSideStackTrace.length + remoteStackTrace.length];
        System.arraycopy(remoteStackTrace, 0, newStackTrace, 0, remoteStackTrace.length);
        newStackTrace[remoteStackTrace.length] = new StackTraceElement(EXCEPTION_SEPARATOR, "", null, -1);
        System.arraycopy(localSideStackTrace, 1, newStackTrace, remoteStackTrace.length + 1, localSideStackTrace.length - 1);
        remoteCause.setStackTrace(newStackTrace);
    }

    public static void fixRemoteStackTrace(Throwable remoteCause, StackTraceElement[] localSideStackTrace
            , String localExceptionMessage) {
        String msg = EXCEPTION_MESSAGE_SEPARATOR.replace("%MSG%", localExceptionMessage);
        StackTraceElement[] remoteStackTrace = remoteCause.getStackTrace();
        StackTraceElement[] newStackTrace = new StackTraceElement[localSideStackTrace.length + remoteStackTrace.length + 1];
        System.arraycopy(remoteStackTrace, 0, newStackTrace, 0, remoteStackTrace.length);
        newStackTrace[remoteStackTrace.length] = new StackTraceElement(EXCEPTION_SEPARATOR, "", null, -1);
        StackTraceElement nextElement = localSideStackTrace[1];
        newStackTrace[remoteStackTrace.length + 1] = new StackTraceElement(msg, nextElement.getMethodName()
                , nextElement.getFileName(), nextElement.getLineNumber());
        System.arraycopy(localSideStackTrace, 1, newStackTrace, remoteStackTrace.length + 2, localSideStackTrace.length - 1);
        remoteCause.setStackTrace(newStackTrace);
    }

}
