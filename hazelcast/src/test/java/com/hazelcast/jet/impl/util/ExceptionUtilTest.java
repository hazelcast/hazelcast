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

package com.hazelcast.jet.impl.util;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
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
            dag.newVertex("source", () -> new MockP().setCompleteError(exc)).localParallelism(1);
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
            dag.newVertex("source", () -> new MockP().setCompleteError(exc)).localParallelism(1);
            client.getJet().newJob(dag).join();
        } catch (Exception caught) {
            assertThat(caught.toString(), containsString(exc.toString()));
        }
    }
}
