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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.metrics.JobMetrics_BatchTest.JOB_CONFIG_WITH_METRICS;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JobMetrics_MiscTest extends TestInClusterSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {
        TestProcessors.reset(MEMBER_COUNT * parallelism);
    }

    @Test
    public void when_jobMetricsDisabled_then_emptyMetrics() throws Throwable {
        DAG dag = new DAG();
        dag.newVertex("v1", MockP::new);
        dag.newVertex("v2", (SupplierEx<Processor>) NoOutputSourceP::new);

        //init
        JobConfig config = new JobConfig()
                .setMetricsEnabled(true) //enable metric collection
                .setStoreMetricsAfterJobCompletion(false); //disable metric saving on completion
        Job job = hz().getJet().newJob(dag, config);

        //when
        NoOutputSourceP.executionStarted.await();
        assertJobStatusEventually(job, JobStatus.RUNNING);
        //then
        assertTrueEventually(() -> assertJobHasMetrics(job, false));

        //when
        NoOutputSourceP.proceedLatch.countDown();
        job.join();
        //then
        assertJobStatusEventually(job, JobStatus.COMPLETED);
        assertEmptyJobMetrics(job, false);
    }

    @Test
    public void when_jobRunning_then_nonEmptyMetrics() throws Throwable {
        DAG dag = new DAG();
        dag.newVertex("v1", MockP::new);
        dag.newVertex("v2", (SupplierEx<Processor>) NoOutputSourceP::new);
        Job job = hz().getJet().newJob(dag, JOB_CONFIG_WITH_METRICS);

        //when
        NoOutputSourceP.executionStarted.await();
        assertJobStatusEventually(job, JobStatus.RUNNING);
        //then
        assertTrueEventually(() -> assertJobHasMetrics(job, false));

        //when
        NoOutputSourceP.proceedLatch.countDown();
        job.join();
        //then
        assertJobStatusEventually(job, JobStatus.COMPLETED);
        assertJobHasMetrics(job, true);
        assertTrue(hz().getMap(JobRepository.JOB_METRICS_MAP_NAME).containsKey(job.getId()));
    }

    @Test
    public void when_jobNotYetRunning_then_emptyMetrics() {
        DAG dag = new DAG();
        BlockingInInitMetaSupplier.latch = new CountDownLatch(1);
        dag.newVertex("v1", new BlockingInInitMetaSupplier());

        Job job = hz().getJet().newJob(dag, JOB_CONFIG_WITH_METRICS);
        assertTrueAllTheTime(() -> assertEmptyJobMetrics(job, false), 2);
        BlockingInInitMetaSupplier.latch.countDown();
        assertTrueEventually(() -> assertJobHasMetrics(job, false));
    }

    @Test
    public void test_duplicateMetricsFromMembers() {
        // A job with a distributed edge causes the presence of distributedBytesIn
        // metric, which doesn't contain the `proc` tag which is unique among
        // members. If there is no special handling for this, then there would
        // be multiple metrics with the same name, causing problems during
        // merging.
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", Processors.noopP());
        Vertex v2 = dag.newVertex("v2", Processors.noopP());
        dag.edge(between(v1, v2).distributed());
        Job job = hz().getJet().newJob(dag, JOB_CONFIG_WITH_METRICS);
        job.join();
        assertJobHasMetrics(job, true);
        // If there would be multiple metrics with the same name, then an
        // assertion error would be thrown when merging them.
    }

    @Test
    public void when_jobSuspended_andMetricsNotStored_then_onlyPeriodicMetricsReturned() throws Throwable {
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", TestProcessors.MockP::new);
        Vertex v2 = dag.newVertex("v2", (SupplierEx<Processor>) TestProcessors.NoOutputSourceP::new);
        dag.edge(between(v1, v2));

        //init
        JobConfig config = new JobConfig()
                .setMetricsEnabled(true) //enable metric collection
                .setStoreMetricsAfterJobCompletion(false); //disable metric saving on completion
        Job job = hz().getJet().newJob(dag, config);

        //when
        TestProcessors.NoOutputSourceP.executionStarted.await();
        //then
        assertJobStatusEventually(job, JobStatus.RUNNING);
        assertTrueEventually(() -> assertJobHasMetrics(job, false));

        //when
        job.suspend();
        //then
        assertJobStatusEventually(job, SUSPENDED);
        assertTrueEventually(() -> assertEmptyJobMetrics(job, false));

        //when
        job.resume();
        //then
        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertJobHasMetrics(job, false));

        //when
        TestProcessors.NoOutputSourceP.proceedLatch.countDown();
        job.join();
        //then
        assertJobStatusEventually(job, JobStatus.COMPLETED);
        assertEmptyJobMetrics(job, false);
    }

    @Test
    public void when_jobRestarted_then_metricsRepopulate() throws Throwable {
        DAG dag = new DAG();
        Vertex v1 = dag.newVertex("v1", TestProcessors.MockP::new);
        Vertex v2 = dag.newVertex("v2", (SupplierEx<Processor>) TestProcessors.NoOutputSourceP::new);
        dag.edge(between(v1, v2));

        Job job = hz().getJet().newJob(dag, JOB_CONFIG_WITH_METRICS);
        TestProcessors.NoOutputSourceP.executionStarted.await();
        assertJobStatusEventually(job, JobStatus.RUNNING);

        job.restart();
        assertJobStatusEventually(job, JobStatus.RUNNING);
        assertTrueEventually(() -> assertJobHasMetrics(job, false));

        TestProcessors.NoOutputSourceP.proceedLatch.countDown();
        job.join();
        assertJobStatusEventually(job, JobStatus.COMPLETED);
        assertJobHasMetrics(job, true);
    }

    @Test
    public void when_metricsForJobDisabled_then_emptyMetrics() throws Throwable {
        DAG dag = new DAG();
        dag.newVertex("v1", MockP::new);
        dag.newVertex("v2", (SupplierEx<Processor>) NoOutputSourceP::new);

        JobConfig config = new JobConfig()
                .setMetricsEnabled(false)
                .setStoreMetricsAfterJobCompletion(true);
        Job job = hz().getJet().newJob(dag, config);

        //when
        NoOutputSourceP.executionStarted.await();
        assertJobStatusEventually(job, JobStatus.RUNNING);
        //then
        assertTrueAllTheTime(() -> assertEmptyJobMetrics(job, false), 2);

        //when
        NoOutputSourceP.proceedLatch.countDown();
        job.join();
        assertJobStatusEventually(job, JobStatus.COMPLETED);
        //then
        assertEmptyJobMetrics(job, true);
    }

    private Job runJobExpectFailure(@Nonnull DAG dag, @Nonnull RuntimeException expectedException) {
        Job job = null;
        try {
            job = hz().getJet().newJob(dag, JOB_CONFIG_WITH_METRICS);
            job.join();
            fail("Job execution should have failed");
        } catch (Exception actual) {
            Throwable cause = peel(actual);
            assertContains(cause.getMessage(), expectedException.getMessage());
        }
        return job;
    }

    private void assertJobHasMetrics(Job job, boolean saved) {
        assertFalse(job.getMetrics().metrics().isEmpty());
        assertFalse(job.getMetrics().get("queuesSize").isEmpty());
        assertEquals(saved, hz().getMap(JobRepository.JOB_METRICS_MAP_NAME).containsKey(job.getId()));
    }

    private void assertEmptyJobMetrics(Job job, boolean saved) {
        assertTrue("Should have been empty, but contained: " + job.getMetrics().metrics(),
                job.getMetrics().metrics().isEmpty());
        assertEquals(saved, hz().getMap(JobRepository.JOB_METRICS_MAP_NAME).containsKey(job.getId()));
    }

    private static class BlockingInInitMetaSupplier implements ProcessorMetaSupplier {
        static CountDownLatch latch;

        @Override
        public void init(@Nonnull Context context) throws Exception {
            latch.await();
        }

        @Nonnull @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return a -> new MockPS(NoOutputSourceP::new, 1);
        }
    }
}
