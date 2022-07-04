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

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.TestUtil.DIRECT_EXECUTOR;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
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
public class ProcessorTaskletTest {

    private static final int MOCK_INPUT_SIZE = 10;
    private static final int CALL_COUNT_LIMIT = 10;

    private List<Object> mockInput;
    private List<MockInboundStream> instreams;
    private List<OutboundEdgeStream> outstreams;
    private PassThroughProcessor processor;
    private Processor.Context context;

    @Before
    public void setUp() {
        this.mockInput = IntStream.range(0, MOCK_INPUT_SIZE).boxed().collect(toList());
        this.processor = new PassThroughProcessor();
        this.context = new TestProcessorContext();
        this.instreams = new ArrayList<>();
        this.outstreams = new ArrayList<>();
    }

    @Test
    public void when_isCooperative_then_true() {
        assertTrue(createTasklet().isCooperative());
    }

    @Test
    public void when_singleInstreamAndOutstream_then_outstreamGetsAll() {
        // Given
        mockInput.add(DONE_ITEM);
        MockInboundStream instream1 = new MockInboundStream(0, mockInput, mockInput.size());
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        instreams.add(instream1);
        outstreams.add(outstream1);
        Tasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, DONE);

        // Then
        assertEquals(mockInput, outstream1.getBuffer());
    }

    @Test
    public void when_oneInstreamAndTwoOutstreams_then_allOutstreamsGetAllItems() {
        // Given
        mockInput.add(DONE_ITEM);
        MockInboundStream instream1 = new MockInboundStream(0, mockInput, mockInput.size());
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        MockOutboundStream outstream2 = new MockOutboundStream(1);
        instreams.add(instream1);
        outstreams.add(outstream1);
        outstreams.add(outstream2);
        Tasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, DONE);

        // Then
        assertEquals(mockInput, outstream1.getBuffer());
        assertEquals(mockInput, outstream2.getBuffer());
    }

    @Test
    public void when_instreamChunked_then_processAllEventually() {
        // Given
        mockInput.add(DONE_ITEM);
        MockInboundStream instream1 = new MockInboundStream(0, mockInput, 4);
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        instreams.add(instream1);
        outstreams.add(outstream1);
        Tasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, DONE);

        // Then
        assertEquals(mockInput, outstream1.getBuffer());
    }

    @Test
    public void when_3instreams_then_pushAllIntoOutstream() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, mockInput.subList(0, 4), 4);
        MockInboundStream instream2 = new MockInboundStream(0, mockInput.subList(4, 8), 4);
        MockInboundStream instream3 = new MockInboundStream(0, mockInput.subList(8, 10), 4);
        instream1.push(DONE_ITEM);
        instream2.push(DONE_ITEM);
        instream3.push(DONE_ITEM);
        instreams.addAll(asList(instream1, instream2, instream3));
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        outstreams.add(outstream1);
        Tasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, DONE);

        // Then
        mockInput.add(DONE_ITEM);
        assertEquals(new HashSet<>(mockInput), new HashSet<>(outstream1.getBuffer()));
    }

    @Test
    public void when_outstreamRefusesItem_then_noProgress() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, mockInput, mockInput.size());
        MockOutboundStream outstream1 = new MockOutboundStream(0, 1);
        instreams.add(instream1);
        outstreams.add(outstream1);
        Tasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertTrue(outstream1.getBuffer().equals(mockInput.subList(0, 1)));
    }

    @Test
    public void when_inboxEmpty_then_nullaryProcessCalled() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, emptyList(), 1);
        MockOutboundStream outstream1 = new MockOutboundStream(0);
        instreams.add(instream1);
        outstreams.add(outstream1);
        ProcessorTasklet tasklet = createTasklet();
        processor.nullaryProcessCallCountdown = 1;

        // When
        callUntil(tasklet, NO_PROGRESS);
        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertTrue("Expected: nullaryProcessCallCountdown<=0, was " + processor.nullaryProcessCallCountdown,
                processor.nullaryProcessCallCountdown <= 0);
    }

    @Test
    public void when_completeReturnsFalse_then_retried() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, singletonList(DONE_ITEM), 1);
        MockOutboundStream outstream1 = new MockOutboundStream(0, 1);
        instreams.add(instream1);
        outstreams.add(outstream1);
        ProcessorTasklet tasklet = createTasklet();
        processor.itemsToEmitInComplete = 2;

        // When

        // first call doesn't immediately detect there is no input
        callUntil(tasklet, NO_PROGRESS);
        // first "completing" item can't be flushed from outbox due to outstream1 constraint
        callUntil(tasklet, NO_PROGRESS);
        outstream1.flush();
        // second "completing" item can't be flushed from outbox due to outstream1 constraint
        callUntil(tasklet, NO_PROGRESS);
        outstream1.flush();
        callUntil(tasklet, DONE);

        // Then
        assertTrue(processor.itemsToEmitInComplete <= 0);
    }

    @Test
    public void when_differentPriorities_then_respected() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, asList(1, 2, DONE_ITEM), 1);
        MockInboundStream instream2 = new MockInboundStream(0, asList(3, 4, DONE_ITEM), 1);
        MockInboundStream instream3 = new MockInboundStream(1, asList(5, 6, DONE_ITEM), 1);
        MockInboundStream instream4 = new MockInboundStream(1, asList(7, 8, DONE_ITEM), 1);
        MockOutboundStream outstream1 = new MockOutboundStream(0, 1);
        instreams.add(instream1);
        instreams.add(instream2);
        instreams.add(instream3);
        instreams.add(instream4);
        outstreams.add(outstream1);
        ProcessorTasklet tasklet = createTasklet();
        processor.itemsToEmitInComplete = 1;
        processor.itemsToEmitInEachCompleteEdge = 1;

        // When

        List<Object> expected = asList(1, 3, 2, 4, "completedEdge=0", "completedEdge=1",
                5, 7, 6, 8, "completedEdge=2", "completedEdge=3");
        List<Object> actual = new ArrayList<>();
        boolean noItemInOutboxAllowed = true;
        while (actual.size() < expected.size()) {
            callUntil(tasklet, MADE_PROGRESS);
            if (outstream1.getBuffer().isEmpty() && noItemInOutboxAllowed) {
                noItemInOutboxAllowed = false;
                continue;
            }
            noItemInOutboxAllowed = true;
            assertEquals("Expected 1 item after " + actual, 1, outstream1.getBuffer().size());
            actual.add(outstream1.getBuffer().remove(0));
        }
        assertEquals(expected, actual);
    }

    @Test
    public void when_closeBlocked_then_waitUntilDone() {
        processor.doneLatch = new CountDownLatch(1);
        ProcessorTasklet tasklet = createTasklet(ForkJoinPool.commonPool());
        callUntil(tasklet, NO_PROGRESS);
        processor.doneLatch.countDown();
        assertTrueEventually(() -> assertEquals(DONE, tasklet.call()), 2);
    }

    private ProcessorTasklet createTasklet() {
        return createTasklet(DIRECT_EXECUTOR);
    }

    private ProcessorTasklet createTasklet(ExecutorService executor) {
        for (int i = 0; i < instreams.size(); i++) {
            instreams.get(i).setOrdinal(i);
        }

        final ProcessorTasklet t = new ProcessorTasklet(context, executor,
                new DefaultSerializationServiceBuilder().build(), processor, instreams, outstreams,
                mock(SnapshotContext.class), new MockOutboundCollector(10), false);
        t.init();
        return t;
    }

    private static class PassThroughProcessor implements Processor {
        int nullaryProcessCallCountdown;
        int itemsToEmitInComplete;
        int itemsToEmitInEachCompleteEdge;
        boolean completeReturnedTrue;
        Set<Integer> completeEdgeReturnedTrue = new HashSet<>();
        private int itemsToEmitInThisCompleteEdge;
        private Outbox outbox;
        private CountDownLatch doneLatch = new CountDownLatch(0);

        @Override
        public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
            this.outbox = outbox;
        }

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            for (Object item; (item = inbox.peek()) != null; ) {
                if (outbox.offer(item)) {
                    inbox.remove();
                } else {
                    return;
                }
            }
        }

        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            return outbox.offer(watermark);
        }

        @Override
        public boolean complete() {
            assertFalse(completeReturnedTrue);
            if (itemsToEmitInComplete > 0 && outbox.offer("completing")) {
                itemsToEmitInComplete--;
            }
            return completeReturnedTrue = itemsToEmitInComplete == 0;
        }

        @Override
        public boolean completeEdge(int ordinal) {
            assertFalse(completeEdgeReturnedTrue.contains(ordinal));
            if (itemsToEmitInThisCompleteEdge == 0) {
                itemsToEmitInThisCompleteEdge = itemsToEmitInEachCompleteEdge;
            }
            if (itemsToEmitInThisCompleteEdge > 0 && outbox.offer("completedEdge=" + ordinal)) {
                itemsToEmitInThisCompleteEdge--;
            }
            if (itemsToEmitInThisCompleteEdge == 0) {
                completeEdgeReturnedTrue.add(ordinal);
                return true;
            }
            return false;
        }

        @Override
        public boolean tryProcess() {
            return nullaryProcessCallCountdown-- <= 0;
        }

        @Override
        public void close() throws InterruptedException {
            doneLatch.await();
        }
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
}
