/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.aggregate.AggregateOperations;
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
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.TestProcessors.ListSource;
import static com.hazelcast.jet.core.TestProcessors.MockP;
import static com.hazelcast.jet.core.TestProcessors.MockPMS;
import static com.hazelcast.jet.core.TestProcessors.MockPS;
import static com.hazelcast.jet.core.metrics.JobMetrics_BatchTest.JOB_CONFIG_WITH_METRICS;
import static com.hazelcast.jet.core.metrics.MetricNames.COALESCED_WM;
import static com.hazelcast.jet.core.metrics.MetricNames.DISTRIBUTED_BYTES_IN;
import static com.hazelcast.jet.core.metrics.MetricNames.DISTRIBUTED_BYTES_OUT;
import static com.hazelcast.jet.core.metrics.MetricNames.DISTRIBUTED_ITEMS_IN;
import static com.hazelcast.jet.core.metrics.MetricNames.DISTRIBUTED_ITEMS_OUT;
import static com.hazelcast.jet.core.metrics.MetricNames.EMITTED_COUNT;
import static com.hazelcast.jet.core.metrics.MetricNames.EXECUTION_COMPLETION_TIME;
import static com.hazelcast.jet.core.metrics.MetricNames.EXECUTION_START_TIME;
import static com.hazelcast.jet.core.metrics.MetricNames.IS_USER_CANCELLED;
import static com.hazelcast.jet.core.metrics.MetricNames.JOB_STATUS;
import static com.hazelcast.jet.core.metrics.MetricNames.LAST_FORWARDED_WM;
import static com.hazelcast.jet.core.metrics.MetricNames.LAST_FORWARDED_WM_LATENCY;
import static com.hazelcast.jet.core.metrics.MetricNames.QUEUES_CAPACITY;
import static com.hazelcast.jet.core.metrics.MetricNames.QUEUES_SIZE;
import static com.hazelcast.jet.core.metrics.MetricNames.RECEIVED_BATCHES;
import static com.hazelcast.jet.core.metrics.MetricNames.RECEIVED_COUNT;
import static com.hazelcast.jet.core.metrics.MetricNames.SNAPSHOT_BYTES;
import static com.hazelcast.jet.core.metrics.MetricNames.SNAPSHOT_KEYS;
import static com.hazelcast.jet.core.metrics.MetricNames.TOP_OBSERVED_WM;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

        assertEquals(MEMBER_COUNT, hzInstances[0].getCluster().getMembers().size());
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
        assertThat(job).eventuallyHasStatus(RUNNING);
        assertTrueEventually(() -> assertJobStatusMetric(job, RUNNING));

        //when
        job.suspend();
        assertThat(job).eventuallyHasStatus(SUSPENDED);

        //then
        assertTrueEventually(() -> assertJobStatusMetric(job, SUSPENDED));
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 0, 0));

        //when
        job.resume();
        assertThat(job).eventuallyHasStatus(RUNNING);

        //then
        assertTrueEventually(() -> assertJobStatusMetric(job, RUNNING));
        assertTrueEventually(() -> assertJobStats(1, 2, 1, 0, 0));
    }

    @Test
    public void jobRestarted() {
        //init
        Job job = hzInstances[0].getJet().newJob(streamingPipeline());
        assertThat(job).eventuallyHasStatus(RUNNING);

        assertTrueEventually(() -> assertJobStats(1, 1, 0, 0, 0));
        assertTrueAllTheTime(() -> assertJobStats(1, 1, 0, 0, 0), 1);

        //when
        job.restart();
        assertThat(job).eventuallyHasStatus(RUNNING);

        //then
        assertTrueEventually(() -> assertJobStats(1, 2, 1, 0, 0));
    }

    @Test
    public void jobCancelled() {
        //init
        Job job = hzInstances[0].getJet().newJob(streamingPipeline(), JOB_CONFIG_WITH_METRICS);
        assertThat(job).eventuallyHasStatus(RUNNING);

        assertTrueEventually(() -> assertJobStats(1, 1, 0, 0, 0));
        assertTrueAllTheTime(() -> assertJobStats(1, 1, 0, 0, 0), 1);

        //when
        job.cancel();

        //then
        assertTrueEventually(() -> assertJobStatusMetric(job, FAILED, true));
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 0, 1));
    }

    @Test
    public void jobSuspendedThenCancelled() {
        //init
        Job job = hzInstances[0].getJet().newJob(streamingPipeline(), JOB_CONFIG_WITH_METRICS);
        assertThat(job).eventuallyHasStatus(RUNNING);

        //when
        job.suspend();
        assertThat(job).eventuallyHasStatus(SUSPENDED);

        //then
        assertTrueEventually(() -> assertJobStatusMetric(job, SUSPENDED));
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 0, 0));

        //when
        job.cancel();
        assertThat(job).eventuallyHasStatus(FAILED);

        //then
        assertTrueEventually(() -> assertJobStatusMetric(job, FAILED, true));
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 0, 1));
    }

    @Test
    public void jobFailed() {
        //init
        Job job = hzInstances[0].getJet().newJob(failingPipeline(), JOB_CONFIG_WITH_METRICS);

        //when
        assertThatThrownBy(job::join).hasRootCauseInstanceOf(ArithmeticException.class);

        //then
        assertTrueEventually(() -> assertJobStatusMetric(job, FAILED));
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 0, 1));
    }

    @Test
    public void executionRelatedMetrics() {
        Job job = hzInstances[0].getJet().newJob(batchPipeline(), JOB_CONFIG_WITH_METRICS);
        job.join();
        assertTrueEventually(() -> assertJobStatusMetric(job, COMPLETED));

        JobMetricsChecker checker = new JobMetricsChecker(job);
        checker.assertRandomMetricValueAtLeast(EXECUTION_START_TIME, 1);
        checker.assertRandomMetricValueAtLeast(EXECUTION_COMPLETION_TIME, 1);
    }

    @Test
    public void jobFinishedAllMetricsPresents() {
        var p = Pipeline.create();
        p.readFrom(TestSources.items(0, 1, 2, 3, 4, 5))
                .groupingKey(i -> i % 2 == 0 ? "odds" : "evens")
                .rollingAggregate(AggregateOperations.counting())
                .writeTo(Sinks.logger());

        var job = hzInstances[0].getJet().newJob(p, createJobConfigWithEnabledMetrics("targetJob"));
        job.join();

        JobMetricsChecker jobChecker = new JobMetricsChecker(job);
        List<String> expectedMetrics = List.of(
                COALESCED_WM,
                EMITTED_COUNT,
                EXECUTION_COMPLETION_TIME,
                EXECUTION_START_TIME,
                LAST_FORWARDED_WM,
                LAST_FORWARDED_WM_LATENCY,
                QUEUES_CAPACITY,
                QUEUES_SIZE,
                RECEIVED_BATCHES,
                RECEIVED_COUNT,
                SNAPSHOT_BYTES,
                SNAPSHOT_KEYS,
                JOB_STATUS,
                IS_USER_CANCELLED,
                TOP_OBSERVED_WM,
                DISTRIBUTED_BYTES_IN,
                DISTRIBUTED_BYTES_OUT,
                DISTRIBUTED_ITEMS_IN,
                DISTRIBUTED_ITEMS_OUT,
                "lateEventsDropped"

        );
        var metrics = job.getMetrics();
        assertEquals(metrics.metrics().size(), expectedMetrics.size());
        expectedMetrics.forEach(jobChecker::assertMetricExists);
    }

    @Test
    public void jobFinishedReceiveMetricsFromTargetJob() {
        hzInstances[0].getJet().newJob(streamingPipeline(), createJobConfigWithEnabledMetrics("otherJob"));
        var job = hzInstances[0].getJet().newJob(batchPipeline(), createJobConfigWithEnabledMetrics("targetJob"));
        job.join();
        var jobName = job.getName();
        var metrics = job.getMetrics();
        metrics.metrics().stream()
                .flatMap(metricName -> metrics.get(metricName).stream())
                .forEach(measurement -> assertEquals(jobName, measurement.tag("jobName")));
    }

    private JobConfig createJobConfigWithEnabledMetrics(String jobName) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setMetricsEnabled(true);
        jobConfig.setStoreMetricsAfterJobCompletion(true);
        jobConfig.setName(jobName);
        return jobConfig;
    }

    private Pipeline batchPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3))
         .writeTo(Sinks.logger());
        return p;
    }

    private Pipeline failingPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3))
                // cause runtime error
                .map(i -> 5 / (i - 2))
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
        assertJobStatusMetric(job, status, false);
    }

    private void assertJobStatusMetric(Job job, JobStatus status, boolean isUserCancelled) {
        try {
            // Check job metrics
            JobMetrics metrics = job.getMetrics();
            List<Measurement> statuses = metrics.get(JOB_STATUS);
            long lastStatus = statuses.get(statuses.size() - 1).value();
            assertEquals(status, JobStatus.getById((int) lastStatus));

            List<Measurement> cancelled = metrics.get(IS_USER_CANCELLED);
            long lastCancelled = cancelled.get(cancelled.size() - 1).value();
            assertEquals(isUserCancelled ? 1 : 0, lastCancelled);

            if (!status.isTerminal()) {
                // Check JMX metrics
                long jmxStatus = JmxMetricsChecker.forJob(hzInstances[0], job)
                        .getMetricValue(JOB_STATUS);
                assertEquals(status, JobStatus.getById((int) jmxStatus));
                // IS_USER_CANCELLED is not reported in JMX metrics by design
            }
        } catch (Exception e) {
            throw new AssertionError(e.getMessage(), e);
        }
    }
}
