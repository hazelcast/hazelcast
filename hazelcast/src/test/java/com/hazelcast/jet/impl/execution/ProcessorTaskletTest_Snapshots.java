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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.operation.SnapshotPhase1Operation.SnapshotPhase1Result;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JetTestSupport.wm;
import static com.hazelcast.jet.core.TestUtil.DIRECT_EXECUTOR;
import static com.hazelcast.jet.impl.MasterJobContext.SNAPSHOT_RESTORE_EDGE_PRIORITY;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
        input.add(barrier0(false));
        MockInboundStream instream1 = new MockInboundStream(0, input, input.size());
        MockOutboundStream outstream1 = new MockOutboundStream(0);

        instreams.add(instream1);
        outstreams.add(outstream1);

        ProcessorTasklet tasklet = createTasklet(ProcessingGuarantee.AT_LEAST_ONCE);

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
        input1.add(barrier0(false));
        input1.addAll(mockInput.subList(4, 8));

        List<Object> input2 = new ArrayList<>();

        MockInboundStream instream1 = new MockInboundStream(0, input1, 1024);
        MockInboundStream instream2 = new MockInboundStream(0, input2, 1024);
        MockOutboundStream outstream1 = new MockOutboundStream(0);

        instreams.add(instream1);
        instreams.add(instream2);
        outstreams.add(outstream1);

        ProcessorTasklet tasklet = createTasklet(EXACTLY_ONCE);

        // When
        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertEquals(asList(0, 1, 2, 3), outstream1.getBuffer());
        assertEquals(emptyList(), getSnapshotBufferValues());

        // When
        instream2.push(barrier0(false));
        callUntil(tasklet, NO_PROGRESS);
        assertEquals(asList(0, 1, 2, 3, barrier0(false), 4, 5, 6, 7), outstream1.getBuffer());
        assertEquals(asList(0, 1, 2, 3, barrier0(false)), getSnapshotBufferValues());
    }

    @Test
    public void when_snapshotTriggered_then_saveSnapshot_prepare_emitBarrier() {
        // Given
        MockOutboundStream outstream1 = new MockOutboundStream(0, 2);
        outstreams.add(outstream1);
        ProcessorTasklet tasklet = createTasklet(EXACTLY_ONCE);
        processor.itemsToEmitInComplete = 4;
        processor.itemsToEmitInSnapshotPrepareCommit = 1;

        callUntil(tasklet, NO_PROGRESS);
        assertEquals(asList(0, 1), outstream1.getBuffer());
        assertEquals(emptyList(), getSnapshotBufferValues());

        // When
        snapshotContext.startNewSnapshotPhase1(0, "map", 0);
        outstream1.flush();

        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertEquals(asList(2, "spc-0"), outstream1.getBuffer());
        assertEquals(asList(0, 1, 2, barrier0(false)), getSnapshotBufferValues());
        outstream1.flush();
        callUntil(tasklet, NO_PROGRESS);
        assertEquals(asList(barrier0(false), 3), outstream1.getBuffer());
    }

    @Test
    public void when_exportOnly_then_commitMethodsNotCalled() {
        // Given
        MockOutboundStream outstream1 = new MockOutboundStream(0, 128);
        outstreams.add(outstream1);
        ProcessorTasklet tasklet = createTasklet(EXACTLY_ONCE);
        processor.itemsToEmitInSnapshotPrepareCommit = 1;
        processor.itemsToEmitInOnSnapshotComplete = 1;
        processor.isStreaming = true;

        // When
        snapshotContext.startNewSnapshotPhase1(0, "map", SnapshotFlags.create(false, true));
        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertEquals(singletonList(barrier0(false)), outstream1.getBuffer());
        snapshotContext.phase1DoneForTasklet(0, 0, 0);
        outstream1.flush();

        // When
        snapshotContext.startNewSnapshotPhase2(0, true);
        callUntil(tasklet, NO_PROGRESS);
        assertEquals(emptyList(), outstream1.getBuffer());
    }

    @Test
    public void when_snapshotRestoreInput_then_restoreMethodsCalled() {
        Entry<String, String> ssEntry1 = entry("k1", "v1");
        Entry<String, String> ssEntry2 = entry("k2", "v2");
        List<Object> restoredSnapshot = asList(ssEntry1, ssEntry2, DONE_ITEM);
        MockInboundStream instream1 = new MockInboundStream(SNAPSHOT_RESTORE_EDGE_PRIORITY, restoredSnapshot, 1024);
        MockInboundStream instream2 = new MockInboundStream(0, singletonList(DONE_ITEM), 1024);
        MockOutboundStream outstream1 = new MockOutboundStream(0);

        instreams.add(instream1);
        instreams.add(instream2);
        outstreams.add(outstream1);

        ProcessorTasklet tasklet = createTasklet(EXACTLY_ONCE);
        processor.itemsToEmitInTryProcess = 1;

        // When
        callUntil(tasklet, DONE);

        // Then
        assertEquals(asList("finishRestore", DONE_ITEM), outstream1.getBuffer());
        assertEquals(singletonList(DONE_ITEM), getSnapshotBufferValues());
        assertEquals(0, processor.tryProcessCount);
    }

    @Test
    public void test_phase2() {
        MockInboundStream instream1 = new MockInboundStream(0, asList("item", barrier0(false)), 1024);
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        instreams.add(instream1);
        outstreams.add(outstream1);

        ProcessorTasklet tasklet = createTasklet(EXACTLY_ONCE);
        processor.itemsToEmitInOnSnapshotComplete = 1;

        callUntil(tasklet, NO_PROGRESS);
        assertEquals(asList("item", barrier0(false)), getSnapshotBufferValues());
        assertEquals(asList("item", barrier0(false)), outstream1.getBuffer());
        snapshotCollector.getBuffer().clear();
        outstream1.flush();

        // start phase 2
        phase1StartAndDone(false);
        CompletableFuture<Void> future = snapshotContext.startNewSnapshotPhase2(0, true);
        callUntil(tasklet, NO_PROGRESS);

        assertEquals(singletonList("osc-0"), outstream1.getBuffer());
        assertEquals(emptyList(), getSnapshotBufferValues());
        assertTrue("future not done", future.isDone());
    }

    @Test
    public void when_processorCompletesAfterPhase1_then_doneAfterPhase2() {
        MockInboundStream instream1 = new MockInboundStream(0, asList("item", barrier0(false), DONE_ITEM), 1024);
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        instreams.add(instream1);
        outstreams.add(outstream1);

        ProcessorTasklet tasklet = createTasklet(EXACTLY_ONCE);
        processor.itemsToEmitInOnSnapshotComplete = 1;

        CompletableFuture<SnapshotPhase1Result> future1 = snapshotContext.startNewSnapshotPhase1(0, "map", 0);
        callUntil(tasklet, NO_PROGRESS);
        assertEquals(asList("item", barrier0(false)), getSnapshotBufferValues());
        assertEquals(asList("item", barrier0(false)), outstream1.getBuffer());
        snapshotContext.phase1DoneForTasklet(1, 1, 1);
        assertTrue("future1 not done", future1.isDone());
        snapshotCollector.getBuffer().clear();
        outstream1.flush();

        // we push an item after DONE_ITEM. This does not happen in reality, but we use it
        // to test that the processor ignores it
        instream1.push("item2");
        callUntil(tasklet, NO_PROGRESS);
        assertEquals(emptyList(), outstream1.getBuffer());

        // start phase 2
        CompletableFuture<Void> future2 = snapshotContext.startNewSnapshotPhase2(0, true);
        callUntil(tasklet, DONE);

        assertEquals(asList("osc-0", DONE_ITEM), outstream1.getBuffer());
        assertEquals(singletonList(DONE_ITEM), getSnapshotBufferValues());
        assertTrue("future2 not done", future2.isDone());
    }

    @Test
    public void when_onSnapshotCompletedReturnsFalse_then_calledAgain() {
        MockInboundStream instream1 = new MockInboundStream(0, singletonList(barrier0(false)), 1024);
        MockOutboundStream outstream1 = new MockOutboundStream(0, 1);
        instreams.add(instream1);
        outstreams.add(outstream1);

        ProcessorTasklet tasklet = createTasklet(EXACTLY_ONCE);
        processor.itemsToEmitInOnSnapshotComplete = 2;

        callUntil(tasklet, NO_PROGRESS);
        assertEquals(singletonList(barrier0(false)), getSnapshotBufferValues());
        assertEquals(singletonList(barrier0(false)), outstream1.getBuffer());
        snapshotCollector.getBuffer().clear();
        outstream1.flush();

        // start phase 2
        phase1StartAndDone(false);
        snapshotContext.startNewSnapshotPhase2(0, true);
        callUntil(tasklet, NO_PROGRESS);
        assertEquals(singletonList("osc-0"), outstream1.getBuffer());
        outstream1.flush();
        callUntil(tasklet, NO_PROGRESS);
        assertEquals(singletonList("osc-1"), outstream1.getBuffer());
    }

    @Test
    public void test_tryProcessWatermark_notInterruptedByPhase2() {
        MockInboundStream instream1 = new MockInboundStream(0, singletonList(wm(0)), 1024);
        MockOutboundStream outstream1 = new MockOutboundStream(0, 1);
        instreams.add(instream1);
        outstreams.add(outstream1);

        ProcessorTasklet tasklet = createTasklet(EXACTLY_ONCE);
        processor.itemsToEmitInTryProcessWatermark = 1;
        callUntil(tasklet, NO_PROGRESS);

        phase1StartAndDone(false);
        CompletableFuture<Void> future = snapshotContext.startNewSnapshotPhase2(0, true);

        callUntil(tasklet, NO_PROGRESS);
        assertFalse("future should not have been done", future.isDone());

        outstream1.flush();
        callUntil(tasklet, NO_PROGRESS);
        assertTrue("future should have been done", future.isDone());

    }

    @Test
    public void test_nullaryTryProcess_notInterruptedByPhase2() {
        MockInboundStream instream1 = new MockInboundStream(0, emptyList(), 1024);
        MockOutboundStream outstream1 = new MockOutboundStream(0, 1);
        instreams.add(instream1);
        outstreams.add(outstream1);

        ProcessorTasklet tasklet = createTasklet(EXACTLY_ONCE);
        processor.itemsToEmitInTryProcess = 2;
        callUntil(tasklet, NO_PROGRESS);

        phase1StartAndDone(false);
        CompletableFuture<Void> future = snapshotContext.startNewSnapshotPhase2(0, true);

        callUntil(tasklet, NO_PROGRESS);
        assertFalse("future should not have been done", future.isDone());

        outstream1.flush();
        callUntil(tasklet, NO_PROGRESS);
        assertTrue("future should have been done", future.isDone());
    }

    @Test
    public void test_terminalSnapshot_receivedInBarrier() {
        MockInboundStream instream1 = new MockInboundStream(0, singletonList(barrier0(true)), 1024);
        MockOutboundStream outstream1 = new MockOutboundStream(0, 128);
        instreams.add(instream1);
        outstreams.add(outstream1);

        ProcessorTasklet tasklet = createTasklet(EXACTLY_ONCE);
        callUntil(tasklet, NO_PROGRESS);

        phase1StartAndDone(false);
        CompletableFuture<Void> future = snapshotContext.startNewSnapshotPhase2(0, true);

        callUntil(tasklet, DONE);
        assertTrue("future should have been done", future.isDone());
        assertEquals(outstream1.getBuffer(), asList(barrier0(true), DONE_ITEM));
    }

    @Test
    public void test_terminalSnapshot_source() {
        MockOutboundStream outstream1 = new MockOutboundStream(0, 128);
        outstreams.add(outstream1);
        processor.isStreaming = true;

        ProcessorTasklet tasklet = createTasklet(EXACTLY_ONCE);
        phase1StartAndDone(true);
        callUntil(tasklet, NO_PROGRESS);

        CompletableFuture<Void> future = snapshotContext.startNewSnapshotPhase2(0, true);

        callUntil(tasklet, DONE);
        assertTrue("future should have been done", future.isDone());
        assertEquals(outstream1.getBuffer(), asList(barrier0(true), DONE_ITEM));
    }

    private ProcessorTasklet createTasklet(ProcessingGuarantee guarantee) {
        for (int i = 0; i < instreams.size(); i++) {
            instreams.get(i).setOrdinal(i);
        }
        snapshotContext = new SnapshotContext(mock(ILogger.class), "test job", -1, guarantee);
        snapshotContext.initTaskletCount(1, 1, 0);
        final ProcessorTasklet t = new ProcessorTasklet(context, DIRECT_EXECUTOR, serializationService,
                processor, instreams, outstreams, snapshotContext, snapshotCollector, false);
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

    private static void callUntil(ProcessorTasklet tasklet, ProgressState expectedState) {
        int iterCount = 0;
        for (ProgressState r; (r = tasklet.call()) != expectedState; ) {
            assertEquals("Failed to make progress after " + iterCount + " iterations", MADE_PROGRESS, r);
            assertTrue(String.format(
                    "tasklet.call() invoked %d times without reaching %s. Last state was %s",
                    CALL_COUNT_LIMIT, expectedState, r),
                    ++iterCount < CALL_COUNT_LIMIT);
        }
    }

    private SnapshotBarrier barrier0(boolean isTerminal) {
        return new SnapshotBarrier(0, isTerminal);
    }

    private void phase1StartAndDone(boolean isTerminal) {
        snapshotContext.startNewSnapshotPhase1(0, "map", SnapshotFlags.create(isTerminal, false));
        snapshotContext.phase1DoneForTasklet(0, 0, 0);
    }

    private static class SnapshottableProcessor implements Processor {

        int itemsToEmitInTryProcess;
        int itemsToEmitInTryProcessWatermark;
        int itemsToEmitInComplete;
        int itemsToEmitInCompleteEdge;
        int itemsToEmitInSnapshotPrepareCommit;
        int itemsToEmitInOnSnapshotComplete;
        int tryProcessCount;
        int tryProcessWatermarkCount;
        int completedCount;
        int completedEdgeCount;
        int snapshotPrepareCommitCount;
        int onSnapshotCompletedCount;
        boolean isStreaming;
        private Outbox outbox;

        private final Queue<Map.Entry> snapshotQueue = new ArrayDeque<>();

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
            if (tryProcessWatermarkCount < itemsToEmitInTryProcessWatermark
                    && outbox.offer("tryProcessWatermark-" + tryProcessWatermarkCount)) {
                tryProcessWatermarkCount++;
            }
            return tryProcessWatermarkCount == itemsToEmitInTryProcessWatermark && outbox.offer(watermark);
        }

        @Override
        public boolean complete() {
            if (completedCount < itemsToEmitInComplete && outbox.offer(completedCount)) {
                snapshotQueue.add(entry(UuidUtil.newUnsecureUUID(), completedCount));
                completedCount++;
            }
            return completedCount == itemsToEmitInComplete && !isStreaming;
        }

        @Override
        public boolean completeEdge(int ordinal) {
            if (completedEdgeCount < itemsToEmitInCompleteEdge && outbox.offer("completeEdge-" + completedEdgeCount)) {
                completedEdgeCount++;
            }
            return completedEdgeCount == itemsToEmitInCompleteEdge;
        }

        @Override
        public boolean tryProcess() {
            if (tryProcessCount < itemsToEmitInTryProcess && outbox.offer("tryProcess-" + tryProcessCount)) {
                tryProcessCount++;
            }
            return tryProcessCount == itemsToEmitInTryProcess;
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
            return true;
        }

        @Override
        public boolean snapshotCommitPrepare() {
            if (snapshotPrepareCommitCount < itemsToEmitInSnapshotPrepareCommit
                    && outbox.offer("spc-" + snapshotPrepareCommitCount)) {
                snapshotPrepareCommitCount++;
            }
            return snapshotPrepareCommitCount == itemsToEmitInSnapshotPrepareCommit;
        }

        @Override
        public boolean snapshotCommitFinish(boolean success) {
            if (onSnapshotCompletedCount < itemsToEmitInOnSnapshotComplete
                    && outbox.offer("osc-" + onSnapshotCompletedCount)) {
                onSnapshotCompletedCount++;
            }
            return onSnapshotCompletedCount == itemsToEmitInOnSnapshotComplete;
        }

        @Override
        public void restoreFromSnapshot(@Nonnull Inbox inbox) {
            for (Object o; (o = inbox.poll()) != null; ) {
                snapshotQueue.add((Entry) o);
            }
        }

        @Override
        public boolean finishSnapshotRestore() {
            return outbox.offer("finishRestore");
        }
    }
}
