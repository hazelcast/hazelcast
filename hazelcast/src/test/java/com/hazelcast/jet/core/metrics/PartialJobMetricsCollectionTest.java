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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.metrics.MetricNames.EXECUTION_COMPLETION_TIME;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test for collecting metrics during partial job completion,
 * where the job has finished processing for some participants while continuing execution for others.
 */
@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartialJobMetricsCollectionTest extends JetTestSupport {

    @Parameterized.Parameters(name = "isStoreMetricsEnabled={0}")
    public static Collection<Object> parameters() {
        return asList(true, false);
    }

    @Parameterized.Parameter
    public boolean isStoreMetricsEnabled;

    private static final int MEMBER_COUNT = 2;

    private HazelcastInstance instance;

    @Before
    public void before() throws Exception {
        TestProcessors.reset(MEMBER_COUNT);

        Config config = smallInstanceConfig();

        var hzInstances = createHazelcastInstances(config, MEMBER_COUNT);
        instance = hzInstances[0];
        assertEquals(MEMBER_COUNT, hzInstances[0].getCluster().getMembers().size());
    }

    @Test
    public void when_jobRunning_then_metricsEventuallyExist() {
        var p = Pipeline.create();
        p.readFrom(TestSources.itemStream(1))
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        JobConfig jobConfig = new JobConfig().setStoreMetricsAfterJobCompletion(isStoreMetricsEnabled);
        var job = instance.getJet().newJob(p, jobConfig);
        assertThat(job).eventuallyHasStatus(RUNNING);
        awaitOnlyOneJobIsRunning(job);

        assertExecutionMetrics(job);

        job.cancel();
    }


    @Test
    public void when_suspendAndResume_then_metricsReset() {
        var p = Pipeline.create();
        p.readFrom(TestSources.itemStream(1))
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        JobConfig jobConfig = new JobConfig().setStoreMetricsAfterJobCompletion(isStoreMetricsEnabled);
        var job = instance.getJet().newJob(p, jobConfig);
        assertThat(job).eventuallyHasStatus(RUNNING);
        awaitOnlyOneJobIsRunning(job);

        job.suspend();
        assertThat(job).eventuallyHasStatus(SUSPENDED);

        assertTrueEventually(() -> assertFalse(job.getMetrics().containsTag(MetricTags.EXECUTION)));

        job.resume();
        assertThat(job).eventuallyHasStatus(RUNNING);

        assertExecutionMetrics(job);

        job.cancel();
    }

    @Test
    public void when_jobRestarted_then_metricsReset() {
        var p = Pipeline.create();
        p.readFrom(TestSources.itemStream(1))
                .withoutTimestamps()
                .writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig().setStoreMetricsAfterJobCompletion(isStoreMetricsEnabled);
        var job = instance.getJet().newJob(p, jobConfig);

        assertThat(job).eventuallyHasStatus(RUNNING);
        awaitOnlyOneJobIsRunning(job);
        var completionTime = job.getMetrics().get(EXECUTION_COMPLETION_TIME).stream().findFirst().orElseThrow().value();
        job.restart();
        assertThat(job).eventuallyHasStatus(RUNNING);

        assertTrueEventually(() -> {
            var metrics = job.getMetrics();
            var completionTimes = metrics.get(EXECUTION_COMPLETION_TIME);
            assertThat(completionTimes).satisfiesOnlyOnce(measurement -> assertThat(measurement.value()).isLessThan(0));
            if (isStoreMetricsEnabled) {
                assertThat(completionTimes).satisfiesOnlyOnce(measurement -> assertThat(measurement.value()).isGreaterThan(completionTime));
            }
        });
        job.cancel();
    }

    @Test
    public void when_jobCanceled_then_terminatedJobMetricsReturn() {
        var p = Pipeline.create();
        p.readFrom(TestSources.itemStream(1))
                .withoutTimestamps()
                .writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig().setStoreMetricsAfterJobCompletion(isStoreMetricsEnabled);
        var job = instance.getJet().newJob(p, jobConfig);
        assertThat(job).eventuallyHasStatus(RUNNING);

        job.cancel();
        assertThat(job).eventuallyHasStatus(FAILED);

        var metrics = job.getMetrics();
        if (isStoreMetricsEnabled) {
            var completionTimes = metrics.get(EXECUTION_COMPLETION_TIME);
            assertThat(completionTimes).hasSize(1);
            assertThat(completionTimes).satisfiesOnlyOnce(measurement -> assertThat(measurement.value()).isGreaterThan(0));
        } else {
            assertThat(metrics.metrics()).isEmpty();
        }
    }

    private void awaitOnlyOneJobIsRunning(Job job) {
        assertTrueEventually(() -> {
            var completionTimes = job.getMetrics().get(EXECUTION_COMPLETION_TIME);
            assertThat(completionTimes).satisfiesOnlyOnce(measurement -> assertThat(measurement.value()).isLessThan(0));
        });
    }

    private void assertExecutionMetrics(Job job) {
        assertTrueEventually(() -> {
            var completionTimes = job.getMetrics().get(EXECUTION_COMPLETION_TIME);
            assertThat(completionTimes).hasSize(isStoreMetricsEnabled ? 2 : 1);
            assertThat(completionTimes).satisfiesOnlyOnce(measurement -> assertThat(measurement.value()).isLessThan(0));
            if (isStoreMetricsEnabled) {
                assertThat(completionTimes).satisfiesOnlyOnce(measurement -> assertThat(measurement.value()).isGreaterThan(0));
            }
        });
    }
}
