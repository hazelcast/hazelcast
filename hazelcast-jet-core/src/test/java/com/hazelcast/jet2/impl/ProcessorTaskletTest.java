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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class ProcessorTaskletTest {

    private List<Integer> list;
    private List<InboundEdgeStream> inboundStreams;
    private List<OutboundEdgeStream> outboundStreams;
    private TestProcessor processor;


    @Before
    public void setUp() throws Exception {
        this.list = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        this.processor = new TestProcessor();
        this.inboundStreams = new ArrayList<>();
        this.outboundStreams = new ArrayList<>();
    }

    @Test
    public void testSingleChunk_when_singleOutput() throws Exception {
        MockInboundStream input1 = new MockInboundStream(list, 10);
        MockOutboundStream output1 = new MockOutboundStream(10);

        inboundStreams.add(input1);
        outboundStreams.add(output1);
        initProcessor();

        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(list, output1.getBuffer());
    }

    @Test
    public void testSingleChunk_when_multipleOutputs() throws Exception {
        MockInboundStream input1 = new MockInboundStream(list, 10);
        MockOutboundStream output1 = new MockOutboundStream(10);
        MockOutboundStream output2 = new MockOutboundStream(10);

        inboundStreams.add(input1);
        outboundStreams.add(output1);
        outboundStreams.add(output2);
        initProcessor();

        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(list, output1.getBuffer());
        assertEquals(list, output2.getBuffer());
    }

    @Test
    public void testProgress_when_multipleChunks() throws Exception {
        MockInboundStream input1 = new MockInboundStream(list, 4);
        MockOutboundStream output1 = new MockOutboundStream(10);

        inboundStreams.add(input1);
        outboundStreams.add(output1);
        initProcessor();

        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(list, output1.getBuffer());
    }

    @Test
    public void testProgress_when_multipleInputs() throws Exception {
        MockInboundStream input1 = new MockInboundStream(Arrays.asList(0, 1, 2, 3), 4);
        MockInboundStream input2 = new MockInboundStream(Arrays.asList(4, 5, 6, 7), 4);
        MockInboundStream input3 = new MockInboundStream(Arrays.asList(8, 9), 4);
        MockOutboundStream output1 = new MockOutboundStream(10);

        inboundStreams.add(input1);
        inboundStreams.add(input2);
        inboundStreams.add(input3);
        outboundStreams.add(output1);
        initProcessor();

        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(new HashSet<>(list), new HashSet<>(output1.getBuffer()));
    }

    @Test
    public void testProgress_when_pendingInputAndOutputEmpty() throws Exception {
        MockInboundStream input1 = new MockInboundStream(list, 4);
        MockOutboundStream output1 = new MockOutboundStream(10);

        inboundStreams.add(input1);
        outboundStreams.add(output1);
        initProcessor();

        Tasklet tasklet = createTasklet();

        processor.paused = true;

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());

        assertTrue("isEmpty", output1.getBuffer().isEmpty());
    }

    private Tasklet createTasklet() {
        return  new ProcessorTasklet(processor, inboundStreams, outboundStreams);
    }

    private void initProcessor() {
        processor.init(new ProcessorContextImpl(), new ArrayDequeOutbox(outboundStreams.size()));
    }

    private static class TestProcessor implements Processor {

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
}
