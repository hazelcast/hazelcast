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

package com.hazelcast.jet.core;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

/**
 * This class tests that if a {@link RestartableException} is thrown in any
 * processor or processor supplier method, the job restarts.
 * <p>
 * For light jobs it tests that the job fails with this exception.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutionLifecycle_RestartableExceptionTest extends SimpleTestInClusterSupport {

    private static final int MEMBER_COUNT = 2;

    private static final RestartableException RESTARTABLE_EXCEPTION =
            new RestartableException("mock restartable exception");

    private static final JobConfig jobConfigWithAutoScaling = new JobConfig().setAutoScaling(true);

    @Parameter
    public boolean useLightJob;

    @Parameters(name = "useLightJob={0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final DAG dag = new DAG();

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setCooperativeThreadCount(2);
        initialize(MEMBER_COUNT, config);
    }

    @Before
    public void before() {
        TestProcessors.reset(MEMBER_COUNT);
        RestartableMockPMS.initCount.set(0);
    }

    @Test
    public void when_inProcessorProcess_cooperative() {
        when_inProcessor(() -> new MockP().setProcessError(RESTARTABLE_EXCEPTION));
    }

    @Test
    public void when_inProcessorProcess_nonCooperative() {
        when_inProcessor(
                () -> new MockP().nonCooperative().setProcessError(RESTARTABLE_EXCEPTION));
    }

    @Test
    public void when_inProcessorComplete_cooperative() {
        when_inProcessor(() -> new MockP().setCompleteError(RESTARTABLE_EXCEPTION));
    }

    @Test
    public void when_inProcessorComplete_nonCooperative() {
        when_inProcessor(
                () -> new MockP().nonCooperative().setCompleteError(RESTARTABLE_EXCEPTION));
    }

    @Test
    public void when_inProcessorInit_cooperative() {
        when_inProcessor(() -> new MockP().setInitError(RESTARTABLE_EXCEPTION));
    }

    @Test
    public void when_inProcessorInit_nonCooperative() {
        when_inProcessor(
                () -> new MockP().nonCooperative().setInitError(RESTARTABLE_EXCEPTION));
    }

    private void when_inProcessor(SupplierEx<Processor> supplier) {
        Vertex src = dag.newVertex("src", () -> new ListSource(1));
        Vertex v = dag.newVertex("v", new MockPS(supplier, MEMBER_COUNT));
        dag.edge(between(src, v));
        Job job = newJob(dag);
        if (useLightJob) {
            assertThatThrownBy(() -> job.join())
                    .hasRootCause(RESTARTABLE_EXCEPTION);
        } else {
            assertTrueEventually(() ->
                    assertGreaterOrEquals("MockPS.init not call count", MockPS.initCount.get(), 2 * MEMBER_COUNT), 10);
        }
    }

    @Test
    public void when_inProcessorSupplierInit() {
        dag.newVertex("v", new MockPS(noopP(), MEMBER_COUNT).setInitError(RESTARTABLE_EXCEPTION));
        when_inProcessorSupplier(dag);
    }

    @Test
    public void when_inProcessorSupplierGet() {
        dag.newVertex("v", new MockPS(noopP(), MEMBER_COUNT).setGetError(RESTARTABLE_EXCEPTION));
        when_inProcessorSupplier(dag);
    }

    private void when_inProcessorSupplier(DAG dag) {
        Job job = newJob(dag);
        if (useLightJob) {
            assertThatThrownBy(() -> job.join())
                    .hasRootCause(RESTARTABLE_EXCEPTION);
        } else {
            assertTrueEventually(() ->
                    assertTrue("MockPS.init not called enough times", MockPS.initCount.get() >= 2 * MEMBER_COUNT), 10);
        }
    }

    @Test
    public void when_inProcessorMetaSupplierInit() {
        dag.newVertex("v", new RestartableMockPMS().setInitError(RESTARTABLE_EXCEPTION));
        whenInProcessorMetaSupplier(dag);
    }

    @Test
    public void when_inProcessorMetaSupplierGet() {
        dag.newVertex("v", new RestartableMockPMS().setGetError(RESTARTABLE_EXCEPTION));
        whenInProcessorMetaSupplier(dag);
    }

    private void whenInProcessorMetaSupplier(DAG dag) {
        Job job = newJob(dag);
        if (useLightJob) {
            assertThatThrownBy(() -> job.join())
                    .hasRootCause(RESTARTABLE_EXCEPTION);
        } else {
            assertTrueEventually(() ->
                    assertTrue("MockPMS.init not called enough times", RestartableMockPMS.initCount.get() > 2), 10);
        }
    }

    private Job newJob(DAG dag) {
        return useLightJob ? instance().getJet().newLightJob(dag) : instance().getJet().newJob(dag, jobConfigWithAutoScaling);
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
