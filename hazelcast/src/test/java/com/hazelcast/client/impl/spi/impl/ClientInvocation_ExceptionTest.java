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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.cp.internal.exception.CannotRemoveCPMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.util.RootCauseMatcher.rootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientInvocation_ExceptionTest extends HazelcastTestSupport {

    private static final int GET = 0;
    private static final int JOIN = 1;
    private static final int JOIN_INTERNAL = 2;

    @Parameterized.Parameters(name = "{0} - {1}")
    public static Object[] parameters() {
        return new Object[]{
                // params: synchronization type, exception thrown, class of expected exception, cause matcher

                //// joinInternal()
                // RuntimeException with a constructor accepting a Throwable cause
                new Object[]{JOIN_INTERNAL, new IllegalStateException("message"), IllegalStateException.class,
                        nullValue(Throwable.class)},
                // RuntimeException with no constructor accepting a Throwable cause
                new Object[]{JOIN_INTERNAL, new IllegalThreadStateException("message"), IllegalThreadStateException.class,
                        nullValue(Throwable.class)},
                new Object[]{JOIN_INTERNAL, new CannotRemoveCPMemberException("message"), CannotRemoveCPMemberException.class,
                        nullValue(Throwable.class)},
                // OperationTimeoutException: OperationTimeoutException is only expected to be
                // thrown with a local stack trace; this test is about verifying the exception remains unwrapped
                new Object[]{JOIN_INTERNAL, new OperationTimeoutException("message"), OperationTimeoutException.class,
                        nullValue(Throwable.class)},
                // CancellationException: CancellationException is only expected to be
                // thrown with a local stack trace; this test is about verifying the exception remains unwrapped
                new Object[]{JOIN_INTERNAL, new CancellationException("message"), CancellationException.class,
                        nullValue(Throwable.class)},
                // Checked exception is wrapped in HazelcastException
                new Object[]{JOIN_INTERNAL, new ClassNotFoundException("message"), HazelcastException.class,
                        rootCause(ClassNotFoundException.class, "message")},
                // Error subclass rethrown as same type
                new Object[]{JOIN_INTERNAL, new OutOfMemoryError("message"), OutOfMemoryError.class,
                        nullValue(Throwable.class)},

                //// join()
                // RuntimeException with a constructor accepting a Throwable cause
                new Object[]{JOIN, new IllegalStateException("message"), CompletionException.class,
                        rootCause(IllegalStateException.class, "message")},
                // RuntimeException with no constructor accepting a Throwable cause
                new Object[]{JOIN, new IllegalThreadStateException("message"), CompletionException.class,
                        rootCause(IllegalThreadStateException.class, "message")},
                new Object[]{JOIN, new CannotRemoveCPMemberException("message"), CompletionException.class,
                        rootCause(CannotRemoveCPMemberException.class, "message")},
                // OperationTimeoutException is wrapped in CompletionException
                new Object[]{JOIN, new OperationTimeoutException("message"), CompletionException.class,
                        rootCause(OperationTimeoutException.class, "message")},
                // CancellationException is expected to be thrown from join() unwrapped
                new Object[]{JOIN, new CancellationException("message"), CancellationException.class,
                        nullValue(Throwable.class)},
                // Checked exception is wrapped in CompletionException
                new Object[]{JOIN, new ClassNotFoundException("message"), CompletionException.class,
                        rootCause(ClassNotFoundException.class, "message")},
                // Error subclass is wrapped in CompletionException
                new Object[]{JOIN, new OutOfMemoryError("message"), CompletionException.class,
                        rootCause(OutOfMemoryError.class, "message")},

                //// get()
                // RuntimeException with a constructor accepting a Throwable cause
                new Object[]{GET, new IllegalStateException("message"), ExecutionException.class,
                        rootCause(IllegalStateException.class, "message")},
                // RuntimeException with no constructor accepting a Throwable cause
                new Object[]{GET, new IllegalThreadStateException("message"), ExecutionException.class,
                        rootCause(IllegalThreadStateException.class, "message")},
                new Object[]{GET, new CannotRemoveCPMemberException("message"), ExecutionException.class,
                        rootCause(CannotRemoveCPMemberException.class, "message")},
                // OperationTimeoutException is wrapped in ExecutionException
                new Object[]{GET, new OperationTimeoutException("message"), ExecutionException.class,
                        rootCause(OperationTimeoutException.class, "message")},
                // CancellationException is expected to be thrown from get() unwrapped
                new Object[]{GET, new CancellationException("message"), CancellationException.class,
                        nullValue(Throwable.class)},
                // Checked exception is wrapped in HazelcastException
                new Object[]{GET, new ClassNotFoundException("message"), ExecutionException.class,
                        rootCause(ClassNotFoundException.class, "message")},
                // Error subclass is wrapped in ExecutionException
                new Object[]{GET, new OutOfMemoryError("message"), ExecutionException.class,
                        rootCause(OutOfMemoryError.class, null)},

        };
    }

    @Parameterized.Parameter
    public int futureSyncMethod;

    @Parameterized.Parameter(1)
    public Throwable exception;

    @Parameterized.Parameter(2)
    public Class<? extends Throwable> expectedExceptionClass;

    @Parameterized.Parameter(3)
    public Condition<? super Throwable> exceptionCauseCondition;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;

    @Before
    public void init() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test() throws Exception {
        IExecutorService executorService = client.getExecutorService("test");

        InternalCompletableFuture f = (InternalCompletableFuture) executorService.submit(new ExceptionThrowingCallable(exception));
        assertCompletesEventually(f);

        assertThatThrownBy(() -> waitForFuture(f, futureSyncMethod))
                .isInstanceOf(expectedExceptionClass)
                .satisfies(t ->  assertThat(t.getCause()).is(exceptionCauseCondition));
    }

    private void waitForFuture(InternalCompletableFuture f, int synchronizationType) throws Exception {
        switch (synchronizationType) {
            case GET:
                f.get();
                break;
            case JOIN:
                f.join();
                break;
            case JOIN_INTERNAL:
                f.joinInternal();
                break;
            default:
                throw new AssertionError("Unknown synchronization type " + synchronizationType);
        }
    }

    public static class ExceptionThrowingCallable implements Callable, Serializable {

        private final Throwable t;

        public ExceptionThrowingCallable(Throwable t) {
            this.t = t;
        }

        @Override
        public Object call() throws Exception {
            if (t instanceof Error) {
                throw (Error) t;
            } else if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Exception) {
                throw (Exception) t;
            }
            throw new AssertionError("Unknown exception type " + t);
        }
    }

    private static <T> Condition<T> nullValue(Class<T> type) {
        return new Condition<>(Objects::isNull, "must be null");
    }
}
