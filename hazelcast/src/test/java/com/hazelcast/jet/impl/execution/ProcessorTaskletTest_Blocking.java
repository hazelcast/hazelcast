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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.TestUtil.DIRECT_EXECUTOR;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProcessorTaskletTest_Blocking {

    private static final int MOCK_INPUT_SIZE = 10;
    private static final int CALL_COUNT_LIMIT = 10;
    private Processor.Context context;
    private List<Object> mockInput;
    private List<MockInboundStream> instreams;
    private List<OutboundEdgeStream> outstreams;
    private PassThroughProcessor processor;


    @Before
    public void setUp() {
        this.processor = new PassThroughProcessor();
        this.context = new TestProcessorContext();
        this.mockInput = IntStream.range(0, MOCK_INPUT_SIZE).boxed().collect(toList());
        this.instreams = new ArrayList<>();
        this.outstreams = new ArrayList<>();
    }

    @Test
    public void when_isCooperative_then_false() {
        assertFalse(createTasklet().isCooperative());
    }

    @Test
    public void when_singleInstreamAndOutstream_then_outstreamGetsAll() {
        // Given
        mockInput.add(DONE_ITEM);
        MockInboundStream instream1 = new MockInboundStream(0, mockInput, mockInput.size());
        MockOutboundStream outstream1 = outstream(0);
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
        MockOutboundStream outstream1 = outstream(0);
        MockOutboundStream outstream2 = outstream(1);
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
        MockOutboundStream outstream1 = outstream(0);
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
        MockOutboundStream outstream1 = new MockOutboundStream(0, 20);
        outstreams.add(outstream1);
        Tasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, DONE);

        // Then
        mockInput.add(DONE_ITEM);
        assertEquals(new HashSet<>(mockInput), new HashSet<>(outstream1.getBuffer()));
    }

    @Test
    public void when_inboxEmpty_then_nullaryProcessCalled() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, emptyList(), 1);
        MockOutboundStream outstream1 = outstream(0);
        instreams.add(instream1);
        outstreams.add(outstream1);
        ProcessorTasklet tasklet = createTasklet();

        // When
        callUntil(tasklet, NO_PROGRESS);
        callUntil(tasklet, NO_PROGRESS);

        // Then
        assertTrue(processor.nullaryProcessCallCount > 0);
    }

    @Test
    public void when_inboxNotEmpty_then_notDone() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, mockInput, mockInput.size());
        MockOutboundStream outstream1 = new MockOutboundStream(0, 1024);
        instreams.add(instream1);
        outstreams.add(outstream1);
        ProcessorTasklet tasklet = createTasklet();
        processor.itemCountToProcess = 1;

        // When
        callUntil(tasklet, NO_PROGRESS);
        instream1.push(DONE_ITEM);
        callUntil(tasklet, DONE);

        // Then
        mockInput.add(DONE_ITEM);
        assertEquals(mockInput, outstream1.getBuffer());
    }

    @Test
    public void when_completeReturnsFalse_then_retried() {
        // Given
        MockInboundStream instream1 = new MockInboundStream(0, Collections.singletonList(DONE_ITEM), 1);
        MockOutboundStream outstream1 = outstream(0);
        instreams.add(instream1);
        outstreams.add(outstream1);
        ProcessorTasklet tasklet = createTasklet();
        processor.itemsToEmitInComplete = 2;

        // When

        // sets instreamCursor to null
        assertEquals(MADE_PROGRESS, tasklet.call());
        // complete() first time
        assertEquals(MADE_PROGRESS, tasklet.call());
        // complete() second time, done
        assertEquals(MADE_PROGRESS, tasklet.call());
        // completeEdge() called
        assertEquals(MADE_PROGRESS, tasklet.call());
        // emit done item, done
        assertEquals(DONE, tasklet.call());

        // Then
        assertTrue(processor.itemsToEmitInComplete == 0);
    }

    // BlockingOutbox tests

    @Test
    public void when_outboxBucketCount_then_equalsOutstreamCount() {
        // Given
        outstreams.add(outstream(0));
        outstreams.add(outstream(1));
        createTasklet();

        // When
        assertEquals(2, processor.outbox.bucketCount());
    }

    @Test
    public void when_emitToOneOrdinal_then_onlyOneBucketFilled() {
        // Given
        MockOutboundStream outstream0 = outstream(0);
        MockOutboundStream outstream1 = outstream(1);
        outstreams.add(outstream0);
        outstreams.add(outstream1);
        processor = new PassThroughProcessor() {
            @Override
            public boolean complete() {
                assertTrue(outbox.offer(1, "completing"));
                return true;
            }
        };
        ProcessorTasklet tasklet = createTasklet();

        // When
        // complete()
        assertEquals(MADE_PROGRESS, tasklet.call());
        // emit done item
        assertEquals(DONE, tasklet.call());
        // Then

        // buffers also contain the DONE_ITEM
        assertTrue("Ordinal 0 received an item", outstream0.getBuffer().size() == 1);
        assertTrue("Ordinal 1 didn't receive an item", outstream1.getBuffer().size() == 2);
    }

    @Test
    public void when_emitToSpecificOrdinals_then_onlyThoseBucketsFilled() {
        // Given
        MockOutboundStream outstream0 = outstream(0);
        MockOutboundStream outstream1 = outstream(1);
        MockOutboundStream outstream2 = outstream(2);
        outstreams.add(outstream0);
        outstreams.add(outstream1);
        outstreams.add(outstream2);
        processor = new PassThroughProcessor() {
            @Override
            public boolean complete() {
                assertTrue(outbox.offer(new int[] {1, 2}, "completing"));
                return true;
            }
        };
        ProcessorTasklet tasklet = createTasklet();

        // When
        // complete()
        assertEquals(MADE_PROGRESS, tasklet.call());
        // emit done item
        assertEquals(DONE, tasklet.call());

        // Then

        // buffers also contain the DONE_ITEM
        assertTrue("Ordinal 0 received an item", outstream0.getBuffer().size() == 1);
        assertTrue("Ordinal 1 didn't receive an item", outstream1.getBuffer().size() == 2);
        assertTrue("Ordinal 2 didn't receive an item", outstream2.getBuffer().size() == 2);
    }

    // END BlockingOutbox tests

    private ProcessorTasklet createTasklet() {
        for (int i = 0; i < instreams.size(); i++) {
            instreams.get(i).setOrdinal(i);
        }
        final ProcessorTasklet t = new ProcessorTasklet(context, DIRECT_EXECUTOR,
                new DefaultSerializationServiceBuilder().build(), processor, instreams, outstreams,
                mock(SnapshotContext.class), new MockOutboundCollector(10), false);
        t.init();
        return t;
    }

    private static class PassThroughProcessor implements Processor {

        Outbox outbox;
        int nullaryProcessCallCount;
        int itemsToEmitInComplete;
        int itemCountToProcess = Integer.MAX_VALUE;

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
            this.outbox = outbox;
        }

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            for (Object item; itemCountToProcess > 0 && (item = inbox.poll()) != null; itemCountToProcess--) {
                emit(item);
            }
            itemCountToProcess = Integer.MAX_VALUE;
        }

        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            return outbox.offer(watermark);
        }

        @Override
        public boolean tryProcess() {
            // A test will set this field to less than zero to provoke an exception in tasklet
            return ++nullaryProcessCallCount > 0;
        }

        @Override
        public boolean complete() {
            if (itemsToEmitInComplete == 0) {
                return true;
            }
            boolean accepted = outbox.offer("completing");
            if (accepted) {
                itemsToEmitInComplete--;
            }
            return itemsToEmitInComplete == 0;
        }

        private void emit(Object item) {
            if (!outbox.offer(item)) {
                throw new AssertionError("Blocking outbox refused an item: " + item);
            }
        }

    }

    private static MockOutboundStream outstream(int ordinal) {
        return new MockOutboundStream(ordinal, 1024);
    }

    private static void callUntil(Tasklet tasklet, ProgressState expectedState) {
        System.out.println("================= call tasklet");
        int iterCount = 0;
        for (ProgressState r; (r = tasklet.call()) != expectedState; ) {
            assertEquals("Failed to make progress", MADE_PROGRESS, r);
            assertTrue(String.format(
                    "tasklet.call() invoked %d times without reaching %s. Last state was %s",
                    CALL_COUNT_LIMIT, expectedState, r),
                    ++iterCount < CALL_COUNT_LIMIT);
            System.out.println("================= call tasklet");
        }
    }
}
