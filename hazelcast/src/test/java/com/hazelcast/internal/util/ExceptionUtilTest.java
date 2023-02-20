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

package com.hazelcast.internal.util;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.util.ExceptionUtil.rethrowFromCollection;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExceptionUtilTest extends HazelcastTestSupport {

    private final Throwable throwable = new RuntimeException("expected exception");

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ExceptionUtil.class);
    }

    @Test
    public void testToString() {
        String result = ExceptionUtil.toString(throwable);

        assertContains(result, "RuntimeException");
        assertContains(result, "expected exception");
    }

    @Test
    public void testPeel_whenThrowableIsRuntimeException_thenReturnOriginal() {
        RuntimeException result = ExceptionUtil.peel(throwable);

        assertEquals(throwable, result);
    }

    @Test
    public void testPeel_whenThrowableIsExecutionException_thenReturnCause() {
        RuntimeException result = ExceptionUtil.peel(new ExecutionException(throwable));

        assertEquals(throwable, result);
    }

    @Test
    public void testPeel_whenThrowableIsExecutionExceptionWithNullCause_thenReturnHazelcastException() {
        ExecutionException exception = new ExecutionException(null);
        RuntimeException result = ExceptionUtil.peel(exception);

        assertTrue(result instanceof HazelcastException);
        assertEquals(exception, result.getCause());
    }

    @Test
    public void testPeel_whenThrowableIsExecutionExceptionWithCustomFactory_thenReturnCustomException() {
        IOException expectedException = new IOException();
        RuntimeException result = (RuntimeException) ExceptionUtil.peel(new ExecutionException(expectedException),
                null, null, (throwable, message) -> new IllegalStateException(message, throwable));

        assertEquals(IllegalStateException.class, result.getClass());
        assertEquals(result.getCause(), expectedException);
    }

    @Test
    public void testCloneExceptionWithFixedAsyncStackTrace_whenStandardConstructorSignature_thenAppendAsyncTrace() {
        IOException expectedException = new IOException();
        ExecutionException result = ExceptionUtil.cloneExceptionWithFixedAsyncStackTrace(new ExecutionException(expectedException));

        assertEquals(ExecutionException.class, result.getClass());
        assertEquals(result.getCause(), expectedException);
    }

    @Test
    public void testCloneExceptionWithFixedAsyncStackTrace_whenNonStandardConstructor_then_cloneReflectively() {
        IOException expectedException = new IOException();
        Throwable result = ExceptionUtil.cloneExceptionWithFixedAsyncStackTrace(
                new NonStandardException(1337, expectedException));

        assertEquals(NonStandardException.class, result.getClass());
        assertEquals(expectedException, result.getCause());
        assertNoAsyncTrace(result);
    }

    @Test
    public void testCloneExceptionWithFixedAsyncStackTrace_whenCannotConstructSource_then_returnWithoutCloning() {
        IOException expectedException = new IOException();
        NoPublicConstructorException result = ExceptionUtil.cloneExceptionWithFixedAsyncStackTrace(
                new NoPublicConstructorException(expectedException));

        assertEquals(NoPublicConstructorException.class, result.getClass());
        assertEquals(expectedException, result.getCause());
        assertNoAsyncTrace(result);
    }

    @Test
    public void testRethrowFromCollection_when_notIgnoredThrowableOnList_then_isRethrown() {
        assertThatExceptionOfType(TestException.class)
                .isThrownBy(() -> {
                    rethrowFromCollection(Collections.singleton(new TestException()));
                });
        assertThatExceptionOfType(TestException.class)
                .isThrownBy(() -> {
                    rethrowFromCollection(Collections.singleton(new TestException()), NullPointerException.class);
                });
        assertThatExceptionOfType(TestException.class)
                .isThrownBy(() -> {
                    rethrowFromCollection(asList(new NullPointerException(), new TestException()), NullPointerException.class);
                });
    }

    @Test
    public void testRethrowFromCollection_when_ignoredThrowableIsOnlyOnList_then_isNotRethrown() throws Throwable {
        rethrowFromCollection(Collections.singleton(new TestException()), TestException.class);
    }

    @Test
    public void testCanCreateExceptionsWithMessageAndCauseWhenExceptionHasCauseSetImplicitlyByNoArgumentConstructor() {
        ExceptionUtil.tryCreateExceptionWithMessageAndCause(
                ExceptionThatHasCauseImplicitlyByNoArgumentConstructor.class, "", new RuntimeException()
        );
    }

    @Test
    public void testCanCreateExceptionsWithMessageAndCauseWhenExceptionHasCauseSetImplicitlyByMessageConstructor() {
        ExceptionUtil.tryCreateExceptionWithMessageAndCause(
                ExceptionThatHasCauseImplicitlyByMessageConstructor.class, "", new RuntimeException()
        );
    }

    private void assertNoAsyncTrace(Throwable result) {
        for (StackTraceElement stackTraceElement : result.getStackTrace()) {
            if (stackTraceElement.getClassName().equals(ExceptionUtil.EXCEPTION_SEPARATOR)) {
                fail("if exception cannot be cloned to append async stack trace, then nothing should be modified");
            }
        }
    }

    private static class NoPublicConstructorException extends RuntimeException {
        private NoPublicConstructorException(Throwable cause) {
            super(cause);
        }
    }

    public static class ExceptionThatHasCauseImplicitlyByNoArgumentConstructor extends RuntimeException {
        public ExceptionThatHasCauseImplicitlyByNoArgumentConstructor() {
            super((Throwable) null);
        }
    }

    public static class ExceptionThatHasCauseImplicitlyByMessageConstructor extends RuntimeException {
        public ExceptionThatHasCauseImplicitlyByMessageConstructor(String message) {
            super(message, null);
        }
    }

    public static class NonStandardException extends RuntimeException {
        private NonStandardException(Integer iDontCareAboutStandardSignatures, Throwable cause) {
            super("" + iDontCareAboutStandardSignatures, cause);
        }
    }

    private static class TestException extends Exception {
    }
}
