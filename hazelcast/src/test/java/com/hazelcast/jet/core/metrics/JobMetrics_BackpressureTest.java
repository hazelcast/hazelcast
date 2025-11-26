/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

// checks whether queue size is correctly updated when back pressure kicks in
@Category({SlowTest.class, ParallelJVMTest.class})
public class JobMetrics_BackpressureTest extends TestInClusterSupport {

    @Test
    public void when_slowTryProcessWatermark_thenQueueSizeUpdated() {
        testWithSlowProcessor(() -> new SlowProcessor().slowTryProcessWatermark());
    }

    @Test
    public void when_slowSaveToSnapshot_thenQueueSizeUpdated() {
        testWithSlowProcessor(() -> new SlowProcessor().slowSaveSnapshot());
    }

    @Test
    public void when_slowSnapshotCommitPrepare_thenQueueSizeUpdated() {
        testWithSlowProcessor(() -> new SlowProcessor().slowSnapshotCommitPrepare());
    }

    @Test
    public void when_slowSnapshotCommitFinish_thenQueueSizeUpdated() {
        testWithSlowProcessor(() -> new SlowProcessor().slowSnapshotCommitFinish());
    }

    private void testWithSlowProcessor(SupplierEx<Processor> slowProcessorSupplier) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.longStream(1000, 1))
                .withIngestionTimestamps()
                // sink is slow and generates backpressure, mapUsingService should propagate it upstream
                .mapUsingService(ServiceFactories.sharedService(__ -> new AtomicLong()), AtomicLong::addAndGet)
                    .setName("map-using-service")
                .writeTo(Sinks.fromProcessor("sink", ProcessorMetaSupplier.of(slowProcessorSupplier)));

        // When
        Job job = hz().getJet().newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
                .setSnapshotIntervalMillis(2000));

        assertTrueEventually(() -> {
            sleepSeconds(1);
            var metrics = job.getMetrics();
            assertThatQueueIsFull(metrics, "sink");
            assertThatQueueIsFull(metrics, "map-using-service");
        });
    }

    private void assertThatQueueIsFull(JobMetrics metrics, String vertexName) {
        long queueCapacity = sumValueFor(metrics, vertexName, MetricNames.QUEUES_CAPACITY);
        long queueSize = sumValueFor(metrics, vertexName, MetricNames.QUEUES_SIZE);
        logger.info(String.format("queue %s: %d/%d", vertexName, queueSize, queueCapacity));
        assertThat(queueCapacity).as("Should initialize metrics fir %s", vertexName).isPositive();
        assertThat(queueSize).as("Should ultimately fill queue for %s", vertexName).isEqualTo(queueCapacity);
    }

    private long sumValueFor(JobMetrics metrics, String vertex, String metric) {
        Collection<Measurement> measurements = metrics
                .filter(MeasurementPredicates.tagValueEquals(MetricTags.VERTEX, vertex)
                            .and(MeasurementPredicates.tagValueEquals(MetricTags.ORDINAL, "snapshot").negate()))
                .get(metric);
        return measurements.stream().mapToLong(Measurement::value).sum();
    }

    private static class SlowProcessor extends AbstractProcessor {

        private boolean tryProcessWatermark = true;
        private boolean saveSnapshot = true;
        private boolean snapshotCommitPrepare = true;
        private boolean snapshotCommitFinish = true;

        @Override
        protected boolean tryProcess0(@NotNull Object item) {
            // sink does not forward items, but consumes them immediately
            return true;
        }

        @Override
        public boolean tryProcessWatermark(@NotNull Watermark watermark) {
            // sink generally does not forward watermarks, so this method should be fast
            // but for the purpose of the test it simulates slow Wm processing
            // e.g. due to downstream processor inbox being full.
            return tryProcessWatermark;
        }

        @Override
        public boolean saveToSnapshot() {
            return saveSnapshot;
        }

        @Override
        public boolean snapshotCommitPrepare() {
            return snapshotCommitPrepare;
        }

        @Override
        public boolean snapshotCommitFinish(boolean success) {
            return snapshotCommitFinish;
        }

        SlowProcessor slowTryProcessWatermark() {
            tryProcessWatermark = false;
            return this;
        }

        SlowProcessor slowSaveSnapshot() {
            saveSnapshot = false;
            return this;
        }

        SlowProcessor slowSnapshotCommitPrepare() {
            snapshotCommitPrepare = false;
            return this;
        }

        SlowProcessor slowSnapshotCommitFinish() {
            snapshotCommitFinish = false;
            return this;
        }
    }
}
