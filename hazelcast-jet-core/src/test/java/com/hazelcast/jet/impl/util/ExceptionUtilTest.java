/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
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
        createJetMember();
        JetInstance client = createJetClient();

        RuntimeException exc = new RuntimeException("myException");
        try {
            DAG dag = new DAG();
            dag.newVertex("source", () -> new MockP().setCompleteError(exc)).localParallelism(1);
            client.newJob(dag).join();
        } catch (Exception caught) {
            assertThat(caught.toString(), containsString(exc.toString()));
            TestUtil.assertExceptionInCauses(exc, caught);
        } finally {
            shutdownFactory();
        }
    }

    @Test
    public void test_serializationOnNode() {
        // create one member and one client
        JetInstance client = createJetMember();

        RuntimeException exc = new RuntimeException("myException");
        try {
            DAG dag = new DAG();
            dag.newVertex("source", () -> new MockP().setCompleteError(exc)).localParallelism(1);
            client.newJob(dag).join();
        } catch (Exception caught) {
            assertThat(caught.toString(), containsString(exc.toString()));
            TestUtil.assertExceptionInCauses(exc, caught);
        }
    }
}
