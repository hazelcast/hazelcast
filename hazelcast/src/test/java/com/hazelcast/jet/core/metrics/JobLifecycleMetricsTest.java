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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.TestProcessors.ListSource;
import static com.hazelcast.jet.core.TestProcessors.MockP;
import static com.hazelcast.jet.core.TestProcessors.MockPMS;
import static com.hazelcast.jet.core.TestProcessors.MockPS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JobLifecycleMetricsTest extends JetTestSupport {

    private static final int MEMBER_COUNT = 2;

    private HazelcastInstance[] hzInstances;

    @Before
    public void before() throws Exception {
        TestProcessors.reset(MEMBER_COUNT);

        Config config = smallInstanceConfig();
        config.setProperty("hazelcast.jmx", "true");
        config.getMetricsConfig().setCollectionFrequencySeconds(1);

        hzInstances = createHazelcastInstances(config, MEMBER_COUNT);
    }

    @After
    public void after() {
        TestProcessors.assertNoErrorsInProcessors();
    }

    @Test
    public void multipleJobsSubmittedAndCompleted() {
        //when
        Job job1 = hzInstances[0].getJet().newJob(batchPipeline());
        job1.join();
        job1.cancel();

        //then
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 1, 0));

        //given
        DAG dag = new DAG();
        Throwable e = new AssertionError("mock error");
        Vertex source = dag.newVertex("source", ListSource.supplier(List.of(1)));

        Vertex process = dag.newVertex("faulty", new MockPMS(() -> new MockPS(() ->
                        new MockP().initBlocks().setProcessError(() -> e), MEMBER_COUNT)))
                .localParallelism(1);
        dag.edge(between(source, process));

        //when
        Job job2 = hzInstances[0].getJet().newJob(dag);

        for (int i = 0; i < MEMBER_COUNT; i++) {
            MockP.unblock();
        }

        try {
            job2.join();
            fail("Expected exception not thrown!");
        } catch (Exception ex) {
            //ignore
        }

        //then
        assertTrueEventually(() -> assertJobStats(2, 2, 2, 1, 1));
    }

    @Test
    public void jobSuspendedThenResumed() {
        //init
        Job job = hzInstances[0].getJet().newJob(streamingPipeline());
        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertJobStatusMetric(job, RUNNING));

        //when
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);

        //then
        assertTrueEventually(() -> assertJobStatusMetric(job, SUSPENDED));
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 0, 0));

        //when
        job.resume();
        assertJobStatusEventually(job, RUNNING);

        //then
        assertTrueEventually(() -> assertJobStatusMetric(job, RUNNING));
        assertTrueEventually(() -> assertJobStats(1, 2, 1, 0, 0));
    }

    @Test
    public void jobRestarted() {
        //init
        Job job = hzInstances[0].getJet().newJob(streamingPipeline());
        assertJobStatusEventually(job, RUNNING);

        assertTrueEventually(() -> assertJobStats(1, 1, 0, 0, 0));
        assertTrueAllTheTime(() -> assertJobStats(1, 1, 0, 0, 0), 1);

        //when
        job.restart();
        assertJobStatusEventually(job, RUNNING);

        //then
        assertTrueEventually(() -> assertJobStats(1, 2, 1, 0, 0));
    }

    @Test
    public void jobCancelled() {
        //init
        Job job = hzInstances[0].getJet().newJob(streamingPipeline(),
                new JobConfig().setStoreMetricsAfterJobCompletion(true));
        assertJobStatusEventually(job, RUNNING);

        assertTrueEventually(() -> assertJobStats(1, 1, 0, 0, 0));
        assertTrueAllTheTime(() -> assertJobStats(1, 1, 0, 0, 0), 1);

        //when
        job.cancel();

        //then
        assertTrueEventually(() -> assertJobStatusMetric(job, FAILED));
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 0, 1));
    }

    @Test
    public void executionRelatedMetrics() {
        Job job = hzInstances[0].getJet().newJob(batchPipeline(),
                new JobConfig().setStoreMetricsAfterJobCompletion(true));
        job.join();
        assertTrueEventually(() -> assertJobStatusMetric(job, COMPLETED));

        JobMetricsChecker checker = new JobMetricsChecker(job);
        checker.assertRandomMetricValueAtLeast(MetricNames.EXECUTION_START_TIME, 1);
        checker.assertRandomMetricValueAtLeast(MetricNames.EXECUTION_COMPLETION_TIME, 1);
    }

    private Pipeline batchPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3))
         .writeTo(Sinks.logger());
        return p;
    }

    private DAG streamingPipeline() {
        DAG dag = new DAG();
        dag.newVertex("v", () -> new MockP().streaming());
        return dag;
    }

    private void assertJobStats(int submitted, int executionsStarted, int executionsTerminated,
                                int completedSuccessfully, int completedWithFailure) {
        assertJobStatsOnMember(hzInstances[0], submitted, executionsStarted, executionsTerminated,
                completedSuccessfully, completedWithFailure);
        for (int i = 1; i < hzInstances.length; i++) {
            assertJobStatsOnMember(hzInstances[i], 0, executionsStarted, executionsTerminated, 0, 0);
        }
    }

    private void assertJobStatsOnMember(HazelcastInstance instance, int submitted, int executionsStarted,
                                        int executionsTerminated, int completedSuccessfully, int completedWithFailure) {
        try {
            JmxMetricsChecker jmxChecker = JmxMetricsChecker.forInstance(instance);
            jmxChecker.assertMetricValue(MetricNames.JOBS_SUBMITTED, submitted);
            jmxChecker.assertMetricValue(MetricNames.JOB_EXECUTIONS_STARTED, executionsStarted);
            jmxChecker.assertMetricValue(MetricNames.JOB_EXECUTIONS_COMPLETED, executionsTerminated);
            jmxChecker.assertMetricValue(MetricNames.JOBS_COMPLETED_SUCCESSFULLY, completedSuccessfully);
            jmxChecker.assertMetricValue(MetricNames.JOBS_COMPLETED_WITH_FAILURE, completedWithFailure);
        } catch (Exception e) {
            throw new AssertionError(e.getMessage(), e);
        }
    }

    private void assertJobStatusMetric(Job job, JobStatus status) {
        try {
            // Check job metrics
            List<Measurement> statuses = job.getMetrics().get(MetricNames.JOB_STATUS);
            long lastStatus = statuses.get(statuses.size() - 1).value();
            assertEquals(status, JobStatus.getById((int) lastStatus));

            if (!status.isTerminal()) {
                // Check JMX metrics
                long jmxStatus = JmxMetricsChecker.forJob(hzInstances[0], job)
                                                  .getMetricValue(MetricNames.JOB_STATUS);
                assertEquals(status, JobStatus.getById((int) jmxStatus));
            }
        } catch (Exception e) {
            throw new AssertionError(e.getMessage(), e);
        }
    }
}
