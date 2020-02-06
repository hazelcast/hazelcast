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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobSnapshotMetricsTest extends SimpleTestInClusterSupport {

    private static final String PREFIX = "com.hazelcast.jet";
    private static final String SOURCE_VERTEX_NAME = "sourceForSnapshot";
    private static final String FILTER_VERTEX_NAME = "filter";

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
    public void when_snapshotCreated_then_snapshotMetricsAreNotEmpty() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        Job job = instance().newJob(pipeline(), jobConfig);

        JobRepository jr = new JobRepository(instance());
        waitForFirstSnapshot(jr, job.getId(), 20, false);

        assertTrueEventually(() -> {
            assertMetricProduced(job, MetricNames.SNAPSHOT_KEYS);
        });
        String mBeanName = getMBeanName(job, SOURCE_VERTEX_NAME);

        assertSnapshotMBeans(mBeanName, 1, true);
    }

    @Test
    public void when_snapshotIsNotCreated_then_snapshotMetricsAreEmpty() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(3600 * 1000);
        Job job = instance().newJob(pipeline(), jobConfig);

        assertTrueEventually(() -> {
            assertMetricProduced(job, MetricNames.SNAPSHOT_KEYS);
        });
        String mBeanName = getMBeanName(job, SOURCE_VERTEX_NAME);

        assertSnapshotMBeans(mBeanName, 0, false);
    }

    @Test
    public void when_snapshotCreated_then_snapshotMetricsAreEmptyForStatelessVertex() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        Job job = instance().newJob(pipeline(), jobConfig);

        JobRepository jr = new JobRepository(instance());
        waitForFirstSnapshot(jr, job.getId(), 20, false);

        assertTrueEventually(() -> {
            assertMetricProduced(job, MetricNames.SNAPSHOT_KEYS);
        });
        String mBeanName = getMBeanName(job, FILTER_VERTEX_NAME);

        assertSnapshotMBeans(mBeanName, 0, false);
    }

    private Pipeline pipeline() {
        Pipeline p = Pipeline.create();
        StreamSource<Long> source = SourceBuilder
                .stream(SOURCE_VERTEX_NAME, procCtx -> new long[1])
                .<Long>fillBufferFn((ctx, buf) -> {
                    buf.add(ctx[0]++);
                    Thread.sleep(5);
                })
                .createSnapshotFn(ctx -> ctx[0])
                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                .build();
        p.readFrom(source)
                .withoutTimestamps()
                .filter(t -> t % 2 == 0)
                .writeTo(Sinks.logger());
        return p;
    }

    private String getMBeanName(Job job, String vertexName) {
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
                .append("\",tag2=\"vertex=")
                .append(vertexName)
                .append("\"");
        return sb.toString();
    }

    private void assertMetricProduced(Job job, String metricName) {
        JobMetrics metrics = job.getMetrics();
        List<Measurement> measurements = metrics.get(metricName);
        assertTrue(measurements.size() > 0);
    }

    private void assertSnapshotMBeans(String name, long expectedSnapshotKeys, boolean snapshotBytesExists)
            throws Exception {
        Set<ObjectInstance> instances = platformMBeanServer.queryMBeans(objectNameWithModule, null);

        ObjectName on = new ObjectName(name);

        Map<ObjectName, ObjectInstance> instanceMap
                = instances.stream().collect(Collectors.toMap(ObjectInstance::getObjectName, Function.identity()));
        assertTrue("name: " + on + " not in instances " + instances, instanceMap.containsKey(on));

        long actualSnapshotKeys = (long) platformMBeanServer.getAttribute(on, MetricNames.SNAPSHOT_KEYS);
        assertEquals("Attribute 'snapshotKeys' of '" + on + "' doesn't match",
                expectedSnapshotKeys, actualSnapshotKeys);

        long actualSnapshotBytes = (long) platformMBeanServer.getAttribute(on, MetricNames.SNAPSHOT_BYTES);
        assertEquals("Attribute 'snapshotBytes' of '" + on + "' doesn't contain expected value",
                snapshotBytesExists, actualSnapshotBytes > 0);
    }

}
