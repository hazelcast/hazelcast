/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.util.UuidUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
public class ProcessorTaskletTest_Snapshots {

    private static final int MOCK_INPUT_SIZE = 10;
    private static final int CALL_COUNT_LIMIT = 10;

    private List<Object> mockInput;
    private List<MockInboundStream> instreams;
    private List<OutboundEdgeStream> outstreams;
    private SnapshottableProcessor processor;
    private Processor.Context context;
    private SerializationService serializationService;
    private SnapshotContext snapshotContext;
    private MockOutboundCollector snapshotCollector;

    @Before
    public void setUp() {
        this.mockInput = IntStream.range(0, MOCK_INPUT_SIZE).boxed().collect(toList());
        this.processor = new SnapshottableProcessor();
        this.serializationService = new DefaultSerializationServiceBuilder().build();
        this.context = new TestProcessorContext().setProcessingGuarantee(EXACTLY_ONCE);
        this.instreams = new ArrayList<>();
        this.outstreams = new ArrayList<>();
        this.snapshotCollector = new MockOutboundCollector(1024);
    }

    @Test
    public void when_isCooperative_then_true() {
        assertTrue(createTasklet(ProcessingGuarantee.AT_LEAST_ONCE).isCooperative());
    }

    @Test
    public void when_singleInbound_then_savesAllToSnapshotAndOutbound() {
        // Given
        List<Object> input = new ArrayList<>();
        input.addAll(mockInput.subList(0, 4));
        input.add(barrier(0));
        MockInboundStream instream1 = new MockInboundStream(0, input, input.size());
        MockOutboundStream outstream1 = new MockOutboundStream(0);

        instreams.add(instream1);
        outstreams.add(outstream1);

        Tasklet tasklet = createTasklet(ProcessingGuarantee.AT_LEAST_ONCE);

        // When
        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertEquals(input, outstream1.getBuffer());
        assertEquals(input, getSnapshotBufferValues());
    }

    @Test
    public void when_multipleInbound_then_waitForBarrier() {
        // Given
        List<Object> input1 = new ArrayList<>();
        input1.addAll(mockInput.subList(0, 4));
        input1.add(barrier(0));
        input1.addAll(mockInput.subList(4, 8));

        List<Object> input2 = new ArrayList<>();

        MockInboundStream instream1 = new MockInboundStream(0, input1, 1024);
        MockInboundStream instream2 = new MockInboundStream(0, input2, 1024);
        MockOutboundStream outstream1 = new MockOutboundStream(0);

        instreams.add(instream1);
        instreams.add(instream2);
        outstreams.add(outstream1);

        Tasklet tasklet = createTasklet(EXACTLY_ONCE);

        // When
        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertEquals(asList(0, 1, 2, 3), outstream1.getBuffer());
        assertEquals(emptyList(), getSnapshotBufferValues());

        // When
        instream2.push(barrier(0));
        callUntil(tasklet, NO_PROGRESS);
        assertEquals(asList(0, 1, 2, 3, barrier(0), 4, 5, 6, 7), outstream1.getBuffer());
        assertEquals(asList(0, 1, 2, 3, barrier(0)), getSnapshotBufferValues());
    }

    @Test
    public void when_snapshotTriggered_then_saveSnapshotAndEmitBarrier() {
        // Given
        MockOutboundStream outstream1 = new MockOutboundStream(0, 2);
        outstreams.add(outstream1);
        Tasklet tasklet = createTasklet(EXACTLY_ONCE);
        processor.itemsToEmitInComplete = 4;

        // When
        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertEquals(asList(0, 1), outstream1.getBuffer());
        assertEquals(emptyList(), getSnapshotBufferValues());

        // When
        snapshotContext.startNewSnapshot(0, "map", false);
        outstream1.flush();

        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertEquals(asList(2, barrier(0)), outstream1.getBuffer());
        assertEquals(asList(0 , 1, 2, barrier(0)), getSnapshotBufferValues());
    }

    @Test
    public void when_snapshotRestoreInput_then_restoreMethodsCalled() {
        Entry<String, String> ssEntry1 = entry("k1", "v1");
        Entry<String, String> ssEntry2 = entry("k2", "v2");
        List<Object> restoredSnapshot = asList(ssEntry1, ssEntry2, DONE_ITEM);
        MockInboundStream instream1 = new MockInboundStream(Integer.MIN_VALUE, restoredSnapshot, 1024);
        MockInboundStream instream2 = new MockInboundStream(0, asList(barrier(0), DONE_ITEM), 1024);
        MockOutboundStream outstream1 = new MockOutboundStream(0);

        instreams.add(instream1);
        instreams.add(instream2);
        outstreams.add(outstream1);

        Tasklet tasklet = createTasklet(EXACTLY_ONCE);

        // When
        callUntil(tasklet, DONE);

        // Then
        assertEquals(asList("finishRestore", barrier(0), DONE_ITEM), outstream1.getBuffer());
        assertEquals(asList(ssEntry1.getValue(), ssEntry2.getValue(), barrier(0), DONE_ITEM), getSnapshotBufferValues());
    }

    private ProcessorTasklet createTasklet(ProcessingGuarantee guarantee) {
        for (int i = 0; i < instreams.size(); i++) {
            instreams.get(i).setOrdinal(i);
        }
        snapshotContext = new SnapshotContext(mock(ILogger.class), "test job", -1, guarantee);
        snapshotContext.initTaskletCount(1, 0);
        final ProcessorTasklet t = new ProcessorTasklet(context, serializationService, processor, instreams, outstreams,
                snapshotContext, snapshotCollector, null);
        t.init();
        return t;
    }

    private List<Object> getSnapshotBufferValues() {
        return snapshotCollector.getBuffer().stream()
                                .map(e -> (e instanceof Map.Entry) ? deserializeEntryValue((Map.Entry) e) : e)
                                .collect(Collectors.toList());
    }

    private Object deserializeEntryValue(Entry e) {
        return serializationService.toObject(e.getValue());
    }

    private static void callUntil(Tasklet tasklet, ProgressState expectedState) {
        int iterCount = 0;
        for (ProgressState r; (r = tasklet.call()) != expectedState; ) {
            assertEquals("Failed to make progress", MADE_PROGRESS, r);
            assertTrue(String.format(
                    "tasklet.call() invoked %d times without reaching %s. Last state was %s",
                    CALL_COUNT_LIMIT, expectedState, r),
                    ++iterCount < CALL_COUNT_LIMIT);
        }
    }

    private SnapshotBarrier barrier(long snapshotId) {
        return new SnapshotBarrier(snapshotId, false);
    }

    private static class SnapshottableProcessor implements Processor {

        int nullaryProcessCallCountdown;
        int itemsToEmitInComplete;
        int completedCount;
        private Outbox outbox;

        private Queue<Map.Entry> snapshotQueue = new ArrayDeque<>();

        @Override
        public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
            this.outbox = outbox;
        }

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            for (Object item; (item = inbox.peek()) != null; ) {
                if (!outbox.offer(item)) {
                    return;
                } else {
                    snapshotQueue.offer(entry(UuidUtil.newUnsecureUUID(), inbox.poll()));
                }
            }
        }

        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            return outbox.offer(watermark);
        }

        @Override
        public boolean complete() {
            if (completedCount < itemsToEmitInComplete && outbox.offer(completedCount)) {
                snapshotQueue.add(entry(UuidUtil.newUnsecureUUID(), completedCount));
                completedCount++;
            }
            return completedCount == itemsToEmitInComplete;
        }

        @Override
        public boolean tryProcess() {
            return nullaryProcessCallCountdown-- <= 0;
        }

        @Override
        public boolean saveToSnapshot() {
            for (Map.Entry item; (item = snapshotQueue.peek()) != null; ) {
                if (!outbox.offerToSnapshot(item.getKey(), item.getValue())) {
                    return false;
                } else {
                    snapshotQueue.remove();
                }
            }
            snapshotQueue.clear();
            return true;
        }

        @Override
        public void restoreFromSnapshot(@Nonnull Inbox inbox) {
            for (Object o; (o = inbox.poll()) != null; ) {
                snapshotQueue.offer((Entry) o);
            }
        }

        @Override
        public boolean finishSnapshotRestore() {
            return outbox.offer("finishRestore");
        }
    }
}
