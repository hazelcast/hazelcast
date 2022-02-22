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

import com.hazelcast.config.Config;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JobSnapshotMetricsTest extends SimpleTestInClusterSupport {

    private static final String SOURCE_VERTEX_NAME = "sourceForSnapshot";
    private static final String FILTER_VERTEX_NAME = "filter";

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        initialize(1, config);
    }

    @Test
    public void when_snapshotCreated_then_snapshotMetricsAreNotEmpty() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        Job job = instance().getJet().newJob(pipeline(), jobConfig);

        JobRepository jr = new JobRepository(instance());
        waitForFirstSnapshot(jr, job.getId(), 20, false);

        JobMetricsChecker checker = new JobMetricsChecker(job);
        assertTrueEventually(() -> checker.assertSummedMetricValueAtLeast(MetricNames.SNAPSHOT_KEYS, 1));

        assertSnapshotMBeans(job, SOURCE_VERTEX_NAME, 1, true);
    }

    @Test
    public void when_snapshotIsNotCreated_then_snapshotMetricsAreEmpty() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(3600 * 1000);
        Job job = instance().getJet().newJob(pipeline(), jobConfig);

        JobMetricsChecker checker = new JobMetricsChecker(job);
        assertTrueEventually(() -> checker.assertSummedMetricValue(MetricNames.SNAPSHOT_KEYS, 0));

        assertSnapshotMBeans(job, SOURCE_VERTEX_NAME, 0, false);
    }

    @Test
    public void when_snapshotCreated_then_snapshotMetricsAreEmptyForStatelessVertex() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        Job job = instance().getJet().newJob(pipeline(), jobConfig);

        JobRepository jr = new JobRepository(instance());
        waitForFirstSnapshot(jr, job.getId(), 20, false);

        JobMetricsChecker checker = new JobMetricsChecker(job);
        assertTrueEventually(() -> checker.assertSummedMetricValueAtLeast(MetricNames.SNAPSHOT_KEYS, 1));

        assertSnapshotMBeans(job, FILTER_VERTEX_NAME, 0, false);
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
                .writeTo(Sinks.noop());
        return p;
    }

    private void assertSnapshotMBeans(Job job, String vertex, long expectedSnapshotKeys, boolean snapshotBytesExists)
            throws Exception {
        JmxMetricsChecker checker = new JmxMetricsChecker(instance().getName(), job, "vertex=" + vertex);
        checker.assertMetricValue(MetricNames.SNAPSHOT_KEYS, expectedSnapshotKeys);

        if (snapshotBytesExists) {
            checker.assertMetricValueAtLeast(MetricNames.SNAPSHOT_BYTES, 1);
        } else {
            checker.assertMetricValue(MetricNames.SNAPSHOT_BYTES, 0);
        }
    }

}
