/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorContext;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.jet2.impl.ProgressState.DONE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class ProcessorTaskletTest {

    private static final int MOCK_INPUT_LENGTH = 10;
    private static final int CALL_COUNT_LIMIT = 10;
    private List<Object> mockInput;
    private List<InboundEdgeStream> instreams;
    private List<OutboundEdgeStream> outstreams;
    private MockProcessor processor;


    @Before
    public void setUp() throws Exception {
        this.mockInput = IntStream.range(0, MOCK_INPUT_LENGTH).boxed().collect(toList());
        this.processor = new MockProcessor();
        this.instreams = new ArrayList<>();
        this.outstreams = new ArrayList<>();
    }

    @Test
    public void when_singleInstreamAndOutstream_then_processAll() throws Exception {
        MockInboundStream instream1 = new MockInboundStream(mockInput, mockInput.size());
        instream1.push(DONE_ITEM);
        MockOutboundStream outstream1 = new MockOutboundStream(1 + mockInput.size());

        instreams.add(instream1);
        outstreams.add(outstream1);
        initProcessor();

        Tasklet tasklet = createTasklet();

        callUntil(tasklet, DONE);
        assertEquals(concat(mockInput.stream(), Stream.of(DONE_ITEM)).collect(toList()), outstream1.getBuffer());
    }

    @Test
    public void testSingleChunk_when_multipleOutputs() throws Exception {
        MockInboundStream input1 = new MockInboundStream(mockInput, 10);
        MockOutboundStream output1 = new MockOutboundStream(10);
        MockOutboundStream output2 = new MockOutboundStream(10);

        instreams.add(input1);
        outstreams.add(output1);
        outstreams.add(output2);
        initProcessor();

        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(mockInput, output1.getBuffer());
        assertEquals(mockInput, output2.getBuffer());
    }

    @Test
    public void testProgress_when_multipleChunks() throws Exception {
        MockInboundStream input1 = new MockInboundStream(mockInput, 4);
        MockOutboundStream output1 = new MockOutboundStream(10);

        instreams.add(input1);
        outstreams.add(output1);
        initProcessor();

        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(mockInput, output1.getBuffer());
    }

    @Test
    public void testProgress_when_multipleInputs() throws Exception {
        MockInboundStream input1 = new MockInboundStream(Arrays.asList(0, 1, 2, 3), 4);
        MockInboundStream input2 = new MockInboundStream(Arrays.asList(4, 5, 6, 7), 4);
        MockInboundStream input3 = new MockInboundStream(Arrays.asList(8, 9), 4);
        MockOutboundStream output1 = new MockOutboundStream(10);

        instreams.add(input1);
        instreams.add(input2);
        instreams.add(input3);
        outstreams.add(output1);
        initProcessor();

        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(new HashSet<>(mockInput), new HashSet<>(output1.getBuffer()));
    }

    @Test
    public void testProgress_when_pendingInputAndOutputEmpty() throws Exception {
        MockInboundStream input1 = new MockInboundStream(mockInput, 4);
        MockOutboundStream output1 = new MockOutboundStream(10);

        instreams.add(input1);
        outstreams.add(output1);
        initProcessor();

        Tasklet tasklet = createTasklet();

        processor.paused = true;

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());

        assertTrue("isEmpty", output1.getBuffer().isEmpty());
    }

    private Tasklet createTasklet() {
        return new ProcessorTasklet(processor, instreams, outstreams);
    }

    private void initProcessor() {
        processor.init(new ProcessorContextImpl(), new ArrayDequeOutbox(outstreams.size()));
    }

    private static class MockProcessor implements Processor {
        private boolean paused;
        private Outbox outbox;

        @Override
        public void init(@Nonnull ProcessorContext context, @Nonnull Outbox outbox) {
            this.outbox = outbox;
        }

        @Override
        public boolean process(int ordinal, Object item) {
            if (paused) {
                return false;
            }
            outbox.add(ordinal, item);
            return true;
        }

        @Override
        public boolean complete() {
            return true;
        }

        @Override
        public boolean complete(int ordinal) {
            return true;
        }
    }

    private static void callUntil(Tasklet tasklet, ProgressState state) throws Exception {
        int iterCount = 0;
        for (ProgressState r; (r = tasklet.call()) != state;) {
            assertTrue("Failed to make progress", r.isMadeProgress());
            assertTrue("tasklet.call() invoked " + CALL_COUNT_LIMIT + " times without reaching " + state
                            + ". Last state was " + r,
                    ++iterCount < CALL_COUNT_LIMIT);
        }
    }
}
