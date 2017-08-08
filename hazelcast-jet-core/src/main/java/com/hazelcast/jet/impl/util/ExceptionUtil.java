/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.TopologyChangedException;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.JET_EXCEPTIONS_RANGE_START;

public final class ExceptionUtil {

    private ExceptionUtil() { }

    public static boolean isJobRestartRequired(Object t) {
        return t instanceof TopologyChangedException
                || t instanceof MemberLeftException
                || t instanceof TargetNotMemberException
                || t instanceof CallerNotMemberException
                || t instanceof HazelcastInstanceNotActiveException;
    }

    /**
     * Called during startup to make our exceptions known to Hazelcast serialization
     */
    public static void registerJetExceptions(@Nonnull ClientExceptionFactory factory) {
        factory.register(JET_EXCEPTIONS_RANGE_START, JetException.class, JetException::new);
        factory.register(JET_EXCEPTIONS_RANGE_START + 1, TopologyChangedException.class, TopologyChangedException::new);
    }

    /**
     * If {@code t} is either of {@link CompletionException}, {@link ExecutionException}
     * or {@link InvocationTargetException}, returns its cause, peeling it recursively.
     * Otherwise returns {@code t}.
     *
     * @param t Throwable to peel
     * @see #peeledAndUnchecked(Throwable)
     */
    public static Throwable peel(@Nullable Throwable t) {
        Throwable peeled = t;
        while ((peeled instanceof CompletionException
                || peeled instanceof ExecutionException
                || peeled instanceof InvocationTargetException)
                && peeled.getCause() != null
        ) {
            peeled = t.getCause();
        }
        return peeled;
    }

    /**
     * Same as {@link #peel(Throwable)}, but additionally if the resulting
     * throwable is not an instance of {@link RuntimeException}, wraps it as
     * {@link JetException}.
     *
     * @param t Throwable to peel
     * @see #peel(Throwable)
     */
    @Nonnull
    private static RuntimeException peeledAndUnchecked(@Nonnull Throwable t) {
        t = peel(t);

        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        }

        return new JetException(t);
    }

    @Nonnull
    public static RuntimeException rethrow(@Nonnull final Throwable t) {
        if (t instanceof Error) {
            if (t instanceof OutOfMemoryError) {
                OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) t);
            }
            throw (Error) t;
        } else {
            throw peeledAndUnchecked(t);
        }
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T extends Throwable> RuntimeException sneakyThrow(@Nonnull Throwable t) throws T {
        throw (T) t;
    }
}
