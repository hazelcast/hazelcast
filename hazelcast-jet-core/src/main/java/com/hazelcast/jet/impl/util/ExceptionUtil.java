/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.clientside.ClientExceptionFactory;
import com.hazelcast.client.impl.clientside.ClientExceptionFactory.ExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientExceptions;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.impl.exception.EnteringPassiveClusterStateException;
import com.hazelcast.jet.impl.exception.ShutdownInProgressException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.JET_EXCEPTIONS_RANGE_START;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;

public final class ExceptionUtil {

    private static final Tuple3<Integer, Class<? extends Throwable>, ExceptionFactory>[] EXCEPTIONS = new Tuple3[] {
            tuple3(JET_EXCEPTIONS_RANGE_START, JetException.class,
                    (ExceptionFactory) JetException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 1, TopologyChangedException.class,
                    (ExceptionFactory) TopologyChangedException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 2, JobNotFoundException.class,
                    (ExceptionFactory) JobNotFoundException::new),
    };

    private ExceptionUtil() { }

    /**
     * Returns true if the exception is one of a kind upon which the job
     * restarts rather than fails.
     */
    public static boolean isRestartableException(Throwable t) {
        return isTopologyException(t)
                || t instanceof RestartableException
                || t instanceof JetException && t.getCause() instanceof RestartableException;
    }

    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public static boolean isTopologyException(Throwable t) {
        return t instanceof TopologyChangedException
                || t instanceof MemberLeftException
                || t instanceof TargetNotMemberException
                || t instanceof CallerNotMemberException
                || t instanceof HazelcastInstanceNotActiveException
                || t instanceof ShutdownInProgressException
                || t instanceof EnteringPassiveClusterStateException;
    }

    /**
     * Called during startup to make our exceptions known to Hazelcast serialization
     */
    public static void registerJetExceptions(@Nonnull ClientExceptions factory) {
        for (Tuple3<Integer, Class<? extends Throwable>, ExceptionFactory> exception : EXCEPTIONS) {
            factory.register(exception.f0(), exception.f1());
        }
    }

    /**
     * Called during startup to make our exceptions known to Hazelcast serialization
     */
    public static void registerJetExceptions(@Nonnull ClientExceptionFactory factory) {
        for (Tuple3<Integer, Class<? extends Throwable>, ExceptionFactory> exception : EXCEPTIONS) {
            factory.register(exception.f0(), exception.f1(), exception.f2());
        }
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
        while ((t instanceof CompletionException
                || t instanceof ExecutionException
                || t instanceof InvocationTargetException)
                && t.getCause() != null
                && t.getCause() != t
        ) {
            t = t.getCause();
        }
        return t;
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

    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T extends Throwable> RuntimeException sneakyThrow(@Nonnull Throwable t) throws T {
        throw (T) t;
    }

    /**
     * If the given exception has "java.lang.ClassCastException: cannot assign
     * instance of java.lang.invoke.SerializedLambda" in the causes, wrap it in
     * another JetException explaining the possible reason.
     * <p>
     * This is a hack to improve readability of this common exception.
     *
     * @param e the exception to handle
     * @return the given exception wrapped, if it is a case of CCE for SerializedLambda
     *     or the given exception otherwise
     */
    public static RuntimeException handleSerializedLambdaCce(HazelcastSerializationException e) {
        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause instanceof ClassCastException
                    && cause.getMessage().startsWith("cannot assign instance of java.lang.invoke.SerializedLambda")) {
                throw new JetException("Class containing the lambda probably missing from class path, did you add it " +
                        "using JobConfig.addClass()?: " + e, e);
            }
            cause = cause.getCause();
        }

        throw e;
    }
}
