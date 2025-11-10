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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.util.ConcurrencyDetection;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_WRITE_QUEUE_PENDING_BYTES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_WRITE_QUEUE_PENDING_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@QuickTest
class NioOutboundPipelineProbeTest {

    @Test
    void testMultipleThreadsWriting()
            throws InterruptedException {
        NioOutboundPipeline underTest = createDummyPipeline();
        int threadCount = 5;
        int framesPerThread = 100000;
        List<Thread> writeThreads = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            writeThreads.add(new Thread(() -> writeRandomSizedFrames(underTest, framesPerThread)));
            writeThreads.get(i).start();
        }
        for (Thread t : writeThreads) {
            t.join();
        }

        for (int i = 0; i < threadCount * framesPerThread; i++) {
            assertThat(underTest.get()).isNotNull();
        }
        assertThat(underTest.get()).isNull();
        Counter pendingBytes = getCounterInstance(underTest, NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_WRITE_QUEUE_PENDING_BYTES);
        assertThat(pendingBytes.get()).isEqualTo(0L);
        Counter pendingPriorityBytes = getCounterInstance(underTest,
                NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_WRITE_QUEUE_PENDING_BYTES);
        assertThat(pendingPriorityBytes.get()).isEqualTo(0L);
    }

    private void writeRandomSizedFrames(NioOutboundPipeline pipeline, int frameCount) {
        Random rng = ThreadLocalRandom.current();
        for (int n = 0; n < frameCount; n++) {
            int size = rng.nextInt(1024, 4096);
            pipeline.write(new DummyFrame(n % 2 == 0, size));
        }
    }

    @Test
    void testPendingBytesGauge() {
        NioOutboundPipeline underTest = createDummyPipeline();

        Counter pendingBytes = getCounterInstance(underTest, NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_WRITE_QUEUE_PENDING_BYTES);
        Counter pendingPriorityBytes = getCounterInstance(underTest,
                NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_WRITE_QUEUE_PENDING_BYTES);

        List<DummyFrame> testFrames = List.of(new DummyFrame(false, 23), new DummyFrame(true, 2), new DummyFrame(false, 3746),
                new DummyFrame(false, 2134), new DummyFrame(true, 3438), new DummyFrame(true, 4857), new DummyFrame(false, 1723),
                new DummyFrame(false, 8659));

        long expectedPending = 0;
        long expectedPriorityPending = 0;

        for (DummyFrame frame : testFrames) {
            underTest.write(frame);
            expectedPending += frame.isUrgent ? 0 : frame.frameLength;
            expectedPriorityPending += frame.isUrgent ? frame.frameLength : 0;
            assertThat(pendingBytes.get()).as("Pending bytes after adding frame %s", frame).isEqualTo(expectedPending);
            assertThat(pendingPriorityBytes.get()).as("Priority pending bytes after adding frame %s", frame).isEqualTo(expectedPriorityPending);
        }

        List<DummyFrame> sortedTestFrames = new ArrayList<>(testFrames);
        sortedTestFrames.sort(Comparator.comparingInt(frame -> frame.isUrgent ? 0 : 1));
        for (DummyFrame frame : sortedTestFrames) {
            assertThat(underTest.get()).isEqualTo(frame);
            expectedPending -= frame.isUrgent ? 0 : frame.frameLength;
            expectedPriorityPending -= frame.isUrgent ? frame.frameLength : 0;
            assertThat(pendingBytes.get()).as("Pending bytes after removing frame %s", frame).isEqualTo(expectedPending);
            assertThat(pendingPriorityBytes.get()).as("Priority pending bytes after removing frame %s", frame).isEqualTo(expectedPriorityPending);
        }
    }

    private NioOutboundPipeline createDummyPipeline() {
        return new NioOutboundPipeline(mock(NioChannel.class), mock(NioThread.class), null, null, null,
                ConcurrencyDetection.createDisabled(), false, false);
    }

    private record DummyFrame(boolean isUrgent, int frameLength)
            implements OutboundFrame {
        @Override
        public boolean isUrgent() {
            return isUrgent;
        }

        @Override
        public int getFrameLength() {
            return frameLength;
        }
    }

    private Counter getCounterInstance(Object obj, String counterName) {
        Field field = Arrays.stream(obj.getClass().getDeclaredFields()).filter(f -> f.getAnnotation(Probe.class) != null)
                            .filter(f -> f.getAnnotation(Probe.class).name().equals(counterName))
                            .filter(f -> Counter.class.isAssignableFrom(f.getType())).findFirst().orElseThrow(
                        () -> new AssertionError("No Counter field with name " + counterName + "on " + obj.getClass()));
        field.setAccessible(true);
        try {
            return (Counter) field.get(obj);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }
}
