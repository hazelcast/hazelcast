/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import net.bytebuddy.implementation.bytecode.Throw;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

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

        assertEquals(result.getClass(), IllegalStateException.class);
        assertEquals(result.getCause(), expectedException);
    }

    @Test
    public void testCloneExceptionWithFixedAsyncStackTrace() {
        IOException expectedException = new IOException();
        ExecutionException result = ExceptionUtil.cloneExceptionWithFixedAsyncStackTrace(new ExecutionException(expectedException));

        assertEquals(result.getClass(), ExecutionException.class);
        assertEquals(result.getCause(), expectedException);
    }

    @Test
    public void testCloneExceptionWithFixedAsyncStackTrace_whenCannotConstructSource_then_returnWithoutCloning() {
        IOException expectedException = new IOException();
        Throwable result = ExceptionUtil.cloneExceptionWithFixedAsyncStackTrace(
                new NonStandardException(1337, expectedException));

        assertEquals(NonStandardException.class, result.getClass());
        assertEquals(expectedException, result.getCause());
        for (StackTraceElement stackTraceElement : result.getStackTrace()) {
            if (stackTraceElement.getClassName().equals(ExceptionUtil.EXCEPTION_SEPARATOR)) {
                fail("if exception cannot be cloned to append async stack trace, then nothing should be modified");
            }
        }
    }

    @Test
    public void testCloneExceptionWithFixedAsyncStackTrace_whenNonStandardConstructor_then_cloneReflectively() {
        IOException expectedException = new IOException();
        NoPublicConstructorException result = ExceptionUtil.cloneExceptionWithFixedAsyncStackTrace(
                new NoPublicConstructorException(expectedException));

        assertEquals(NoPublicConstructorException.class, result.getClass());
        assertEquals(expectedException, result.getCause());
    }

    public static class NoPublicConstructorException extends RuntimeException {
        private NoPublicConstructorException(Throwable cause) {
            super(cause);
        }
    }
    public static class NonStandardException extends RuntimeException {
        private NonStandardException(Integer iDontCareAboutStandardSignatures, Throwable cause) {
            super("" + iDontCareAboutStandardSignatures, cause);
        }
    }
}
