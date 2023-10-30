/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.ClientExceptionFactory.ExceptionFactory;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JobAlreadyExistsException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.impl.exception.CancellationByUserException;
import com.hazelcast.jet.impl.exception.EnteringPassiveClusterStateException;
import com.hazelcast.jet.impl.exception.JetDisabledException;
import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;
import com.hazelcast.jet.impl.exception.TerminatedWithSnapshotException;
import com.hazelcast.jet.impl.execution.TaskletExecutionException;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.jet.impl.operation.StartExecutionOperation;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.sql.impl.ResultLimitReachedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.JET_EXCEPTIONS_RANGE_START;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;

public final class ExceptionUtil {
    private static final List<Tuple3<Integer, Class<? extends Throwable>, ExceptionFactory>> EXCEPTIONS = Arrays.asList(
            tuple3(JET_EXCEPTIONS_RANGE_START, JetException.class, JetException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 1, TopologyChangedException.class, TopologyChangedException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 2, JobNotFoundException.class, JobNotFoundException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 3, JobAlreadyExistsException.class, JobAlreadyExistsException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 4, AssertionCompletedException.class, AssertionCompletedException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 5, JetDisabledException.class, JetDisabledException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 6, CancellationByUserException.class, CancellationByUserException::new),
            tuple3(JET_EXCEPTIONS_RANGE_START + 7, TaskletExecutionException.class, TaskletExecutionException::new)
    );

    private ExceptionUtil() { }

    /**
     * Returns true if the exception is one of a kind upon which the job
     * restarts rather than fails.
     */
    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public static boolean isRestartableException(Throwable t) {
        return isTopologyException(t)
                || (t instanceof RestartableException)
                || (t instanceof JetException && isOrHasCause(t, RestartableException.class))
                || (t instanceof CompletionException && isOrHasCause(t, RestartableException.class))
                ;
    }

    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    public static boolean isTopologyException(Throwable t) {
        return t instanceof TopologyChangedException
                || t instanceof MemberLeftException
                || t instanceof TargetNotMemberException
                || t instanceof HazelcastInstanceNotActiveException
                || t instanceof EnteringPassiveClusterStateException
                || t instanceof OperationTimeoutException
                    && (t.getMessage().contains(InitExecutionOperation.class.getSimpleName())
                        || t.getMessage().contains(StartExecutionOperation.class.getSimpleName()));
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
        com.hazelcast.internal.util.ExceptionUtil.rethrowIfError(t);
        throw peeledAndUnchecked(t);
    }

    @Nonnull
    public static String stackTraceToString(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
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

    /**
     * Checks, if {@code t} itself or any exception in its cause chain is an
     * instance of {@code classToFind}.
     */
    public static boolean isOrHasCause(Throwable t, Class<?> classToFind) {
        while (t != null && t.getCause() != t && !classToFind.isAssignableFrom(t.getClass())) {
            t = t.getCause();
        }
        return t != null && classToFind.isAssignableFrom(t.getClass());
    }

    public static boolean isTechnicalCancellationException(Throwable t) {
        Throwable peeledFailure = peel(t);
        return peeledFailure instanceof JobTerminateRequestedException
                || peeledFailure instanceof ResultLimitReachedException
                || peeledFailure instanceof TerminatedWithSnapshotException
                || checkCause(peeledFailure);
    }

    private static boolean checkCause(Throwable peeledFailure) {
        return peeledFailure.getCause() != null
                && peeledFailure != peeledFailure.getCause()
                && isTechnicalCancellationException(peeledFailure.getCause());
    }
}
