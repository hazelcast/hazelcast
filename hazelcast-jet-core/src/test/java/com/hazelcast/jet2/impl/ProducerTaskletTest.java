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
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class ProducerTaskletTest {

    private List<Integer> list;
    private ListProducer producer;
    private List<OutboundEdgeStream> outboundStreams;

    @Before
    public void setup() {
        this.list = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        this.producer = new ListProducer(list, 1024);
        producer.setBatchSize(4);
        this.outboundStreams = new ArrayList<>();
    }

    @Test
    public void testSingleChunk_whenSingleOutput() throws Exception {
        MockOutboundStream output1 = new MockOutboundStream(10);
        outboundStreams.add(output1);
        Tasklet tasklet = createTasklet();
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1, 2, 3), output1.getBuffer());
    }

    @Test
    public void testSingleChunk_whenMultipleOutputs() throws Exception {
        MockOutboundStream output1 = new MockOutboundStream(10);
        MockOutboundStream output2 = new MockOutboundStream(10);
        outboundStreams.add(output1);
        outboundStreams.add(output2);

        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1, 2, 3), output1.getBuffer());
        assertEquals(Arrays.asList(0, 1, 2, 3), output2.getBuffer());
    }

    @Test
    public void testAllChunks_whenSingleOutput() throws Exception {
        MockOutboundStream output1 = new MockOutboundStream(10);
        outboundStreams.add(output1);

        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.DONE, tasklet.call());

        assertEquals(list, output1.getBuffer());
    }

    @Test
    public void testAllChunks_whenMultipleOutputs() throws Exception {
        MockOutboundStream output1 = new MockOutboundStream(10);
        MockOutboundStream output2 = new MockOutboundStream(10);
        outboundStreams.add(output1);
        outboundStreams.add(output2);

        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.DONE, tasklet.call());

        assertEquals(list, output1.getBuffer());
        assertEquals(list, output2.getBuffer());
    }

    @Test
    public void testProgress_whenOutputIsFull() throws Exception {
        MockOutboundStream output1 = new MockOutboundStream(4);
        outboundStreams.add(output1);
        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.NO_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(0, 1, 2, 3), output1.drain());

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.NO_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(4, 5, 6, 7), output1.drain());

        assertEquals(ProgressState.DONE, tasklet.call());

        assertEquals(Arrays.asList(8, 9), output1.drain());
    }

    @Test
    public void testProgress_whenOutputFullThenFullyDrained() throws Exception {
        MockOutboundStream output1 = new MockOutboundStream(1);
        outboundStreams.add(output1);
        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.NO_PROGRESS, tasklet.call());
        assertEquals(singletonList(0), output1.drain());

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.NO_PROGRESS, tasklet.call());

        assertEquals(singletonList(1), output1.getBuffer());
    }

    @Test
    public void testProgress_whenOnlyOneOutputFull() throws Exception {
        MockOutboundStream output1 = new MockOutboundStream(2);
        MockOutboundStream output2 = new MockOutboundStream(4);
        outboundStreams.add(output1);
        outboundStreams.add(output2);
        Tasklet tasklet = createTasklet();


        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(ProgressState.NO_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(0, 1), output1.getBuffer());
        assertEquals(Arrays.asList(0, 1), output2.getBuffer());
    }

    @Test
    public void testNoProgress_whenProducerIdle() throws Exception {
        MockOutboundStream output1 = new MockOutboundStream(10);
        outboundStreams.add(output1);
        Tasklet tasklet = createTasklet();
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1, 2, 3), output1.drain());

        producer.pause();

        assertEquals(ProgressState.NO_PROGRESS, tasklet.call());

        producer.resume();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(4, 5, 6, 7), output1.drain());
    }

    @Test
    public void testDone_whenProducerIdleAndComplete() throws Exception {
        MockOutboundStream output1 = new MockOutboundStream(10);
        outboundStreams.add(output1);
        Tasklet tasklet = createTasklet();
        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1, 2, 3), output1.drain());

        producer.completeEarly();

        assertEquals(ProgressState.DONE, tasklet.call());

        assertTrue(output1.drain().isEmpty());
    }

    @Test
    public void testProgress_whenProducerIdleButPendingOutput() throws Exception {
        MockOutboundStream output1 = new MockOutboundStream(2);
        outboundStreams.add(output1);
        Tasklet tasklet = createTasklet();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1), output1.drain());
        producer.pause();

        assertEquals(ProgressState.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(2, 3), output1.drain());

    }

    @Test
    public void testIsBlocking() {
        ProcessorTasklet tasklet =
                new ProcessorTasklet(new Processor() {
                    @Override
                    public void init(@Nonnull ProcessorContext context, @Nonnull Outbox collector) { }
                    @Override
                    public boolean process(int ordinal, Object item) {
                        return false;
                    }
                    @Override
                    public boolean complete(int ordinal) {
                        return false;
                    }
                    @Override
                    public boolean complete() {
                        return false;
                    }
                    @Override
                    public boolean isBlocking() {
                        return true;
                    }
                }, emptyList(), outboundStreams);
        assertTrue(tasklet.isBlocking());
    }

    private ProcessorTasklet createTasklet() {
        return new ProcessorTasklet(producer, emptyList(), outboundStreams);
    }
}
