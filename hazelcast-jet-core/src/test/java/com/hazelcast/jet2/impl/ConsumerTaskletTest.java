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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class ConsumerTaskletTest {

    private List list;
    private List<InboundEdgeStream> inboundStreams;
    private ListConsumer consumer;

    @Before
    public void setup() {
        this.list = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        this.consumer = new ListConsumer();
        this.inboundStreams = new ArrayList<>();
    }

    @Test
    public void testSingleChunk_when_singleInput() throws Exception {
        MockInboundStream input1 = new MockInboundStream(4, list);

        inboundStreams.add(input1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1, 2, 3), consumer.getList());
        assertFalse("isComplete", consumer.isComplete());

    }

    @Test
    public void testAllChunks_when_singleInput() throws Exception {
        MockInboundStream input1 = new MockInboundStream(4, list);
        inboundStreams.add(input1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(list, consumer.getList());
        assertTrue("isComplete", consumer.isComplete());

    }

    @Test
    public void testProgress_when_singleInputNotComplete() throws Exception {
        MockInboundStream input1 = new MockInboundStream(list.size(), list);
        inboundStreams.add(input1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(list, consumer.getList());
        assertFalse("isComplete", consumer.isComplete());

    }

    @Test
    public void testProgress_when_singleInputNewData() throws Exception {
        MockInboundStream input1 = new MockInboundStream(list.size(), list);
        inboundStreams.add(input1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());

        input1.push(10, 11);

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), consumer.getList());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_singleInputNoProgress() throws Exception {
        MockInboundStream input1 = new MockInboundStream(list.size(), list);
        inboundStreams.add(input1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        input1.pause();
        assertEquals(TaskletResult.NO_PROGRESS, tasklet.call());
        assertFalse("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_multipleInput() throws Exception {
        MockInboundStream input1 = new MockInboundStream(list.size(), list);
        MockInboundStream input2 = new MockInboundStream(list.size(), list);
        inboundStreams.add(input1);
        inboundStreams.add(input2);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(list.size() * 2, consumer.getList().size());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_multipleInput_oneFinishedEarlier() throws Exception {
        MockInboundStream input1 = new MockInboundStream(2, Arrays.asList(1, 2));
        MockInboundStream input2 = new MockInboundStream(list.size(), list);
        inboundStreams.add(input1);
        inboundStreams.add(input2);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(12, consumer.getList().size());
        assertTrue("isComplete", consumer.isComplete());
    }


    @Test
    public void testProgress_when_consumerYields() throws Exception {
        MockInboundStream input1 = new MockInboundStream(10, list);
        inboundStreams.add(input1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1), consumer.getList());
        assertFalse("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_consumerYieldsOnSameItem() throws Exception {
        MockInboundStream input1 = new MockInboundStream(10, list);
        inboundStreams.add(input1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        consumer.yieldOn(2);
        assertEquals(TaskletResult.NO_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1), consumer.getList());
        assertFalse("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_consumerYieldsAgain() throws Exception {
        MockInboundStream input1 = new MockInboundStream(10, list);
        inboundStreams.add(input1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());

        consumer.yieldOn(4);
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(0, 1, 2, 3), consumer.getList());

        assertTrue(tasklet.call().isDone());

        assertEquals(list, consumer.getList());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_consumerYieldsAndThenRuns() throws Exception {
        MockInboundStream input1 = new MockInboundStream(10, list);
        inboundStreams.add(input1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(list, consumer.getList());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_consumerYieldsAndNoInput() throws Exception {
        MockInboundStream input1 = new MockInboundStream(3, list);
        inboundStreams.add(input1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());

        input1.pause();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(0, 1, 2), consumer.getList());
        assertFalse("isComplete", consumer.isComplete());
    }

    @Test
    public void testIsBlocking() {
        inboundStreams.add(new MockInboundStream(10, list));
        ProcessorTasklet tasklet =
                new ProcessorTasklet(new Processor() {
                    @Override
                    public void init(@Nonnull ProcessorContext context, @Nonnull Outbox outbox) {

                    }
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
        }, emptyList(), emptyList());
        assertTrue(tasklet.isBlocking());
    }

    private Tasklet createTasklet() {
        return new ProcessorTasklet(consumer, inboundStreams, emptyList());
    }
}
