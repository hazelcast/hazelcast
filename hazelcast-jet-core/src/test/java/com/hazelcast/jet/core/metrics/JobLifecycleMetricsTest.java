/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.TestProcessors.ListSource;
import static com.hazelcast.jet.core.TestProcessors.MockP;
import static com.hazelcast.jet.core.TestProcessors.MockPMS;
import static com.hazelcast.jet.core.TestProcessors.MockPS;
import static com.hazelcast.jet.core.TestProcessors.reset;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JobLifecycleMetricsTest extends JetTestSupport {

    private static final int MEMBER_COUNT = 2;

    private JetInstance[] jetInstances;

    @Before
    public void before() throws Exception {
        reset(MEMBER_COUNT);

        JetConfig config = new JetConfig();
        config.setProperty("hazelcast.jmx", "true");
        config.configureHazelcast(hzConfig -> hzConfig.getMetricsConfig().setCollectionFrequencySeconds(1));

        jetInstances = createJetMembers(config, MEMBER_COUNT);
    }

    @After
    public void after() {
        Jet.shutdownAll();
    }

    @Test
    public void multipleJobsSubmittedAndCompleted() {
        //when
        Job job1 = jetInstances[0].newJob(batchPipeline());
        job1.join();
        job1.cancel();

        //then
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 1, 0));

        //given
        DAG dag = new DAG();
        Throwable e = new AssertionError("mock error");
        Vertex source = dag.newVertex("source", ListSource.supplier(singletonList(1)));
        Vertex process = dag.newVertex("faulty",
                new MockPMS(() -> new MockPS(() -> new MockP().setProcessError(e), MEMBER_COUNT)));
        dag.edge(between(source, process));

        //when
        Job job2 = jetInstances[0].newJob(dag);
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
        Job job = jetInstances[0].newJob(streamingPipeline());
        assertJobStatusEventually(job, RUNNING);

        //when
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);

        //then
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 0, 0));

        //when
        job.resume();
        assertJobStatusEventually(job, RUNNING);

        //then
        assertTrueEventually(() -> assertJobStats(1, 2, 1, 0, 0));
    }

    @Test
    public void jobRestarted() {
        //init
        Job job = jetInstances[0].newJob(streamingPipeline());
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
        Job job = jetInstances[0].newJob(streamingPipeline());
        assertJobStatusEventually(job, RUNNING);

        assertTrueEventually(() -> assertJobStats(1, 1, 0, 0, 0));
        assertTrueAllTheTime(() -> assertJobStats(1, 1, 0, 0, 0), 1);

        //when
        job.cancel();

        //then
        assertTrueEventually(() -> assertJobStats(1, 1, 1, 0, 1));
    }

    @Test
    public void executionRelatedMetrics() {
        Job job = jetInstances[0].newJob(batchPipeline(), new JobConfig().setStoreMetricsAfterJobCompletion(true));
        job.join();

        JobMetricsChecker checker = new JobMetricsChecker(job);
        long startTime = checker.assertRandomMetricValueAtLeast(MetricNames.EXECUTION_START_TIME, 1);
        long completionTime = checker.assertRandomMetricValueAtLeast(MetricNames.EXECUTION_COMPLETION_TIME, 1);

        assertTrue("startTime=" + startTime + ", completionTime=" + completionTime,
                startTime <= completionTime);
    }

    private Pipeline batchPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3))
                .writeTo(Sinks.logger());
        return p;
    }

    private Pipeline streamingPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(3))
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        return p;
    }

    private void assertJobStats(int submitted, int executionsStarted, int executionsTerminated,
                                int completedSuccessfully, int completedWithFailure) {
        assertJobStatsOnMember(jetInstances[0], submitted, executionsStarted, executionsTerminated,
                completedSuccessfully, completedWithFailure);
        for (int i = 1; i < jetInstances.length; i++) {
            assertJobStatsOnMember(jetInstances[i], 0, executionsStarted, executionsTerminated, 0, 0);
        }
    }

    private void assertJobStatsOnMember(JetInstance jetInstance, int submitted, int executionsStarted,
                                   int executionsTerminated, int completedSuccessfully, int completedWithFailure) {
        try {
            JmxMetricsChecker jmxChecker = new JmxMetricsChecker(jetInstance.getName());
            jmxChecker.assertMetricValue(MetricNames.JOBS_SUBMITTED, submitted);
            jmxChecker.assertMetricValue(MetricNames.JOB_EXECUTIONS_STARTED, executionsStarted);
            jmxChecker.assertMetricValue(MetricNames.JOB_EXECUTIONS_COMPLETED, executionsTerminated);
            jmxChecker.assertMetricValue(MetricNames.JOBS_COMPLETED_SUCCESSFULLY, completedSuccessfully);
            jmxChecker.assertMetricValue(MetricNames.JOBS_COMPLETED_WITH_FAILURE, completedWithFailure);
        } catch (AssertionError ae) {
            throw ae;
        } catch (Exception e) {
            throw new AssertionError(e.getMessage(), e);
        }
    }
}
