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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.TestSources;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.core.JetTestSupport.assertJobStatusEventually;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobExecutionMetricsTest extends SimpleTestInClusterSupport {

    private static final String PREFIX = "com.hazelcast.jet";
    private static final long JOB_HAS_NOT_FINISHED_YET_TIME = -1;

    private ObjectName objectNameWithModule;

    private MBeanServer platformMBeanServer;

    @BeforeClass
    public static void beforeClass() {
        JetConfig config = new JetConfig();
        config.configureHazelcast(hzConfig -> hzConfig.getMetricsConfig().setCollectionFrequencySeconds(1));
        initialize(1, config);
    }

    @Before
    public void before() throws Exception {
        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        objectNameWithModule = new ObjectName(PREFIX + ":*");
    }

    @Test
    public void testExecutionMetricsBatchJob() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setStoreMetricsAfterJobCompletion(true);
        Job job = instance().newJob(batchPipeline(), jobConfig);
        job.join();

        assertTrueEventually(() -> {
            assertMetricProduced(job, MetricNames.EXECUTION_START_TIME);
        });

        JobMetrics metrics = job.getMetrics();
        long executionStartTime = metrics.get(MetricNames.EXECUTION_START_TIME).get(0).value();
        assertTrue(executionStartTime > 0);
        long executionCompletionTime = metrics.get(MetricNames.EXECUTION_COMPLETION_TIME).get(0).value();
        assertTrue(executionCompletionTime >= executionStartTime);
    }

    @Test
    public void testExecutionMetricsStreamJob() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setStoreMetricsAfterJobCompletion(true);
        Job job = instance().newJob(streamPipeline(), jobConfig);

        assertTrueEventually(() -> {
            assertMetricProduced(job, MetricNames.EXECUTION_START_TIME);
        });

        ObjectName on = getObjectName(getMBeanName(job));

        long executionStartTime = assertMBeanAfterJobStartAndGetExecutionStartTime(on);

        job.cancel();
        assertJobStatusEventually(job, FAILED);

        JobMetrics metrics = job.getMetrics();
        long executionStartTimeAfterCancel = metrics.get(MetricNames.EXECUTION_START_TIME).get(0).value();
        assertEquals(executionStartTime, executionStartTimeAfterCancel);
        long executionCompletionTimeAfterCancel = metrics.get(MetricNames.EXECUTION_COMPLETION_TIME).get(0).value();
        assertTrue(executionCompletionTimeAfterCancel >= executionStartTime);
    }

    @Test
    public void testExecutionMetricsJobRestart() throws Exception {
        Job job = instance().newJob(streamPipeline());

        assertTrueEventually(() -> {
            assertMetricProduced(job, MetricNames.EXECUTION_START_TIME);
        });

        ObjectName on = getObjectName(getMBeanName(job));

        long executionStartTime = assertMBeanAfterJobStartAndGetExecutionStartTime(on);

        job.restart();

        long executionStartTimeAfterCancel = getMBeanAttribute(on, MetricNames.EXECUTION_START_TIME);
        assertEquals(executionStartTime, executionStartTimeAfterCancel);
        long executionCompletionTimeAfterCancel = getMBeanAttribute(on, MetricNames.EXECUTION_COMPLETION_TIME);
        assertEquals(JOB_HAS_NOT_FINISHED_YET_TIME, executionCompletionTimeAfterCancel);
    }

    @Test
    public void testExecutionMetricsSuspendResumeWithSnapshot() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        Job job = instance().newJob(snapshotPipeline(), jobConfig);

        JobRepository jr = new JobRepository(instance());
        waitForFirstSnapshot(jr, job.getId(), 20, false);

        assertTrueEventually(() -> {
            assertMetricProduced(job, MetricNames.EXECUTION_START_TIME);
        });

        ObjectName on = getObjectName(getMBeanName(job));

        long executionStartTime = assertMBeanAfterJobStartAndGetExecutionStartTime(on);

        job.restart();

        long executionStartTimeAfterCancel = getMBeanAttribute(on, MetricNames.EXECUTION_START_TIME);
        assertEquals(executionStartTime, executionStartTimeAfterCancel);
        long executionCompletionTimeAfterCancel = getMBeanAttribute(on, MetricNames.EXECUTION_COMPLETION_TIME);
        assertEquals(JOB_HAS_NOT_FINISHED_YET_TIME, executionCompletionTimeAfterCancel);
    }

    private Pipeline streamPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(20))
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        return p;
    }

    private Pipeline batchPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(0, 1, 2, 3, 4))
                .writeTo(Sinks.logger());
        return p;
    }

    private Pipeline snapshotPipeline() {
        Pipeline p = Pipeline.create();
        StreamSource<Long> source = SourceBuilder
                .stream("src", procCtx -> new long[1])
                .<Long>fillBufferFn((ctx, buf) -> {
                    buf.add(ctx[0]++);
                    Thread.sleep(5);
                })
                .createSnapshotFn(ctx -> ctx[0])
                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                .build();
        p.readFrom(source)
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        return p;
    }

    private String getMBeanName(Job job) {
        String jobId = job.getIdString();
        String execId = job.getMetrics().get(MetricNames.SNAPSHOT_KEYS).get(0).tag("exec");
        StringBuilder sb = new StringBuilder();
        sb.append(PREFIX)
                .append(":type=Metrics,instance=")
                .append(instance().getName())
                .append(",tag0=\"job=")
                .append(jobId)
                .append("\",tag1=\"exec=")
                .append(execId)
                .append("\"");
        return sb.toString();
    }

    private void assertMetricProduced(Job job, String metricName) {
        JobMetrics metrics = job.getMetrics();
        List<Measurement> measurements = metrics.get(metricName);
        assertTrue(measurements.size() > 0);
    }

    private long assertMBeanAfterJobStartAndGetExecutionStartTime(ObjectName on) throws Exception {
        long executionStartTime = getMBeanAttribute(on, MetricNames.EXECUTION_START_TIME);
        assertTrue(executionStartTime > 0);
        long executionCompletionTime = getMBeanAttribute(on, MetricNames.EXECUTION_COMPLETION_TIME);
        assertEquals(JOB_HAS_NOT_FINISHED_YET_TIME, executionCompletionTime);
        return executionStartTime;
    }

    private ObjectName getObjectName(String name) throws Exception {
        Set<ObjectInstance> instances = platformMBeanServer.queryMBeans(objectNameWithModule, null);

        ObjectName on = new ObjectName(name);

        Map<ObjectName, ObjectInstance> instanceMap
                = instances.stream().collect(Collectors.toMap(ObjectInstance::getObjectName, Function.identity()));
        assertTrue("name: " + on + " not in instances " + instances, instanceMap.containsKey(on));

        return on;
    }

    private long getMBeanAttribute(ObjectName on, String name) throws Exception {
        return (long) platformMBeanServer.getAttribute(on, name);
    }
}
