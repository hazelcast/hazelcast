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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.isOrHasCause;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExceptionUtilTest extends JetTestSupport {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void when_throwableIsRuntimeException_then_peelReturnsOriginal() {
        Throwable throwable = new RuntimeException("expected exception");
        Throwable result = peel(throwable);

        assertEquals(throwable, result);
    }

    @Test
    public void when_throwableIsExecutionException_then_peelReturnsCause() {
        Throwable throwable = new RuntimeException("expected exception");
        Throwable result = peel(new ExecutionException(throwable));

        assertEquals(throwable, result);
    }

    @Test
    public void when_throwableIsExecutionExceptionWithNullCause_then_returnHazelcastException() {
        ExecutionException exception = new ExecutionException(null);
        exceptionRule.expect(JetException.class);
        throw rethrow(exception);
    }

    @Test
    public void test_serializationFromNodeToClient() {
        // create one member and one client
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();

        RuntimeException exc = new RuntimeException("myException");
        try {
            DAG dag = new DAG();
            dag.newVertex("source", () -> new MockP().setCompleteError(() -> exc)).localParallelism(1);
            client.getJet().newJob(dag).join();
        } catch (Exception caught) {
            assertContains(caught.toString(), exc.toString());
        }
    }

    @Test
    public void test_serializationOnNode() {
        // create one member and one client
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();

        RuntimeException exc = new RuntimeException("myException");
        try {
            DAG dag = new DAG();
            dag.newVertex("source", () -> new MockP().setCompleteError(() -> exc)).localParallelism(1);
            client.getJet().newJob(dag).join();
        } catch (Exception caught) {
            assertThat(caught.toString()).contains(exc.toString());
        }
    }

    @Test
    public void test_isOrHasCause_when_expectedTypeADeepCause_then_true() {
        Throwable throwable = new TargetNotMemberException("");
        for (int i = 0; i < 10; i++) {
            throwable = new Exception(throwable);
        }
        assertTrue(isOrHasCause(throwable, TargetNotMemberException.class));
    }

    @Test
    public void test_isOrHasCause_when_noMatch_then_false() {
        Throwable throwable = new Exception();
        for (int i = 0; i < 10; i++) {
            throwable = new Exception(throwable);
        }
        assertFalse(isOrHasCause(throwable, TargetNotMemberException.class));
    }

    @Test
    public void test_isOrHasCause_when_selfCause() {
        Throwable throwable = new Exception() {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };
        assertFalse(isOrHasCause(throwable, TargetNotMemberException.class));
    }

    @Test
    public void test_isOrHasCause_null() {
        assertFalse(isOrHasCause(null, TargetNotMemberException.class));
    }

    @Test
    public void test_isOrHasCause_when_exceptionHasExpectedType() {
        RuntimeException e = new RuntimeException("foo");
        assertTrue(isOrHasCause(e, RuntimeException.class));
    }

    @Test
    public void test_stackTraceToString() {
        Exception exception1 = new Exception("exception1");
        Exception exception2 = new Exception("exception2", exception1);
        Exception exception3 = new Exception("exception3", exception2);

        String stackTrace = ExceptionUtil.stackTraceToString(exception3);
        assertThat(stackTrace).contains("exception3", "exception2", "exception1");
    }
}
