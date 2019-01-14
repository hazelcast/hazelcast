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

package com.hazelcast.jet.core;

import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class ExecutionLifecycle_RestartableExceptionTest extends TestInClusterSupport {

    private static final RestartableException RESTARTABLE_EXCEPTION =
            new RestartableException("mock restartable exception");

    private static JobConfig jobConfigWithAutoScaling = new JobConfig().setAutoScaling(true);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before() {
        TestProcessors.reset(MEMBER_COUNT);
        RestartableMockPMS.initCount.set(0);
    }

    @Test
    public void when_inProcessorProcess_then_jobRestarted_cooperative() {
        when_inProcessorMethod_then_jobRestarted(() -> new MockP().setProcessError(RESTARTABLE_EXCEPTION));
    }

    @Test
    public void when_inProcessorProcess_then_jobRestarted_nonCooperative() {
        when_inProcessorMethod_then_jobRestarted(
                () -> new MockP().nonCooperative().setProcessError(RESTARTABLE_EXCEPTION));
    }

    @Test
    public void when_inProcessorComplete_then_jobRestarted_cooperative() {
        when_inProcessorMethod_then_jobRestarted(() -> new MockP().setCompleteError(RESTARTABLE_EXCEPTION));
    }

    @Test
    public void when_inProcessorComplete_then_jobRestarted_nonCooperative() {
        when_inProcessorMethod_then_jobRestarted(
                () -> new MockP().nonCooperative().setCompleteError(RESTARTABLE_EXCEPTION));
    }

    @Test
    public void when_inProcessorInit_then_jobRestarted_cooperative() {
        when_inProcessorMethod_then_jobRestarted(() -> new MockP().setInitError(RESTARTABLE_EXCEPTION));
    }

    @Test
    public void when_inProcessorInit_then_jobRestarted_nonCooperative() {
        when_inProcessorMethod_then_jobRestarted(
                () -> new MockP().nonCooperative().setInitError(RESTARTABLE_EXCEPTION));
    }

    private void when_inProcessorMethod_then_jobRestarted(DistributedSupplier<Processor> supplier) {
        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", () -> new ListSource(1));
        Vertex v = dag.newVertex("v", new MockPS(supplier, MEMBER_COUNT));
        dag.edge(between(src, v));
        member.newJob(dag, jobConfigWithAutoScaling);
        assertTrueEventually(() ->
                assertTrue("MockPS.init not called enough times", MockPS.initCount.get() >= 2 * MEMBER_COUNT), 10);
    }

    @Test
    public void when_inProcessorSupplierInit_then_jobRestarted() {
        DAG dag = new DAG();
        dag.newVertex("v", new MockPS(noopP(), MEMBER_COUNT).setInitError(RESTARTABLE_EXCEPTION));
        member.newJob(dag, jobConfigWithAutoScaling);
        assertTrueEventually(() ->
                assertTrue("MockPS.init not called enough times", MockPS.initCount.get() >= 2 * MEMBER_COUNT), 10);
    }

    @Test
    public void when_inProcessorSupplierGet_then_jobRestarted() {
        DAG dag = new DAG();
        dag.newVertex("v", new MockPS(noopP(), MEMBER_COUNT).setGetError(RESTARTABLE_EXCEPTION));
        member.newJob(dag, jobConfigWithAutoScaling);
        assertTrueEventually(() ->
                assertTrue("MockPS.close not called enough times", MockPS.closeCount.get() >= 2 * MEMBER_COUNT), 10);
    }

    @Test
    public void when_inProcessorMetaSupplierInit_then_jobRestarted() {
        DAG dag = new DAG();
        dag.newVertex("v", new RestartableMockPMS().setInitError(RESTARTABLE_EXCEPTION));
        member.newJob(dag, jobConfigWithAutoScaling);
        assertTrueEventually(() ->
                assertTrue("MockPMS.init not called enough times", RestartableMockPMS.initCount.get() > 2), 10);
    }

    @Test
    public void when_inProcessorMetaSupplierGet_then_jobRestarted() {
        DAG dag = new DAG();
        dag.newVertex("v", new RestartableMockPMS().setGetError(RESTARTABLE_EXCEPTION));
        member.newJob(dag, jobConfigWithAutoScaling);
        assertTrueEventually(() ->
                assertTrue("MockPMS.init not called enough times", RestartableMockPMS.initCount.get() > 2), 10);
    }

    private static class RestartableMockPMS implements ProcessorMetaSupplier {
        static final AtomicInteger initCount = new AtomicInteger();

        private RuntimeException initError;
        private RuntimeException getError;

        RestartableMockPMS setInitError(RuntimeException initError) {
            this.initError = initError;
            return this;
        }

        RestartableMockPMS setGetError(RuntimeException getError) {
            this.getError = getError;
            return this;
        }

        @Override
        public void init(@Nonnull Context context) {
            initCount.incrementAndGet();
            if (initError != null) {
                throw initError;
            }
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            if (getError != null) {
                throw getError;
            }
            throw new AssertionError("should never get here");
        }
    }
}
