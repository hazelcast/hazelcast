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

package com.hazelcast.test.metrics;

import com.hazelcast.internal.metrics.MetricConsumer;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;

/**
 * {@link MetricsPublisher} implementation that records the last N metrics
 * blobs compressed into a buffer to dump later on request. Used by the
 * {@link MetricsRule} that requests dumping the recorded metrics on test
 * failure.
 */
class TestMetricPublisher implements MetricsPublisher {
    private final byte[][] blobs;
    private final long[] timestamps;
    private final MetricsCompressor compressor = new MetricsCompressor();
    private final int slots;
    private final SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss,SSS");
    private volatile int seq = 0;
    private volatile boolean recordedMetrics;

    TestMetricPublisher(int slots) {
        this.slots = slots;
        this.blobs = new byte[slots][];
        this.timestamps = new long[slots];
    }

    @Override
    public void publishLong(MetricDescriptor descriptor, long value) {
        compressor.addLong(descriptor, value);
    }

    @Override
    public void publishDouble(MetricDescriptor descriptor, double value) {
        compressor.addDouble(descriptor, value);
    }

    @Override
    public void whenComplete() {
        synchronized (blobs) {
            int nextSlot = seq++ % slots;
            blobs[nextSlot] = compressor.getBlobAndReset();
            timestamps[nextSlot] = System.currentTimeMillis();
        }
        recordedMetrics = true;
    }

    void dumpRecordings(String instanceName, StringBuilder sb) {
        // if there is no recording we wait for bounded time to record one blob with metrics
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (!recordedMetrics && System.currentTimeMillis() < deadline) {
            sleepMillis(250);
        }

        sb.append("\n");

        final int oldestSlotIdx;
        final byte[][] slotsCopy = new byte[slots][];
        synchronized (blobs) {
            oldestSlotIdx = seq % slots;
            System.arraycopy(blobs, 0, slotsCopy, 0, slots);
        }

        for (int i = 0; i < slots; i++) {
            int currentSlot = (oldestSlotIdx + i) % slots;
            byte[] blob = slotsCopy[currentSlot];
            if (blob != null) {
                Date date = new Date(timestamps[currentSlot]);
                MetricsCompressor.extractMetrics(blob, new MetricConsumer() {
                    @Override
                    public void consumeLong(MetricDescriptor descriptor, long value) {
                        appendMetric(sb, instanceName, date, descriptor).append(value).append("\n");
                    }

                    @Override
                    public void consumeDouble(MetricDescriptor descriptor, double value) {
                        appendMetric(sb, instanceName, date, descriptor).append(value).append("\n");
                    }

                    private StringBuilder appendMetric(StringBuilder sb, String instanceName, Date date,
                                                       MetricDescriptor descriptor) {
                        return sb.append('[').append(instanceName).append("] ")
                                .append('[').append(formatter.format(date)).append("] ")
                                .append(descriptor.metricString()).append('=');
                    }
                });
            }
        }
    }

    @Override
    public String name() {
        return "TestMetricPublisher";
    }
}
