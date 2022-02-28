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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
}
