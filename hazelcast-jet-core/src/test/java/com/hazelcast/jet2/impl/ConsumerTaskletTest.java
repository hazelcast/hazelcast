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
import java.util.stream.IntStream;

import static com.hazelcast.jet2.impl.TaskletResult.DONE;
import static com.hazelcast.jet2.impl.TaskletResult.MADE_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.NO_PROGRESS;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class ConsumerTaskletTest {

    private static final int MOCK_INPUT_LENGTH = 10;
    private static final int CALL_COUNT_LIMIT = 10;
    private List<Object> mockInput;
    private List<InboundEdgeStream> inboundStreams;
    private ListConsumer consumer;

    @Before
    public void setup() {
        this.mockInput = IntStream.range(0, MOCK_INPUT_LENGTH).boxed().collect(toList());
        this.consumer = new ListConsumer();
        this.inboundStreams = new ArrayList<>();
    }

    @Test
    public void when_oneInboundStream_then_consumeAllAndComplete() throws Exception {
        // Given
        MockInboundStream stream1 = new MockInboundStream(mockInput, 1 + mockInput.size());
        inboundStreams.add(stream1);
        Tasklet tasklet = createTasklet();

        // When
        callUntilDone(tasklet);

        // Then
        assertTrue("isComplete", consumer.isComplete());
        assertEquals(mockInput, consumer.getList());
    }

    @Test
    public void when_exhaustInputAndComplete_then_done() throws Exception {
        // Given
        final int chunkSize = 4;
        MockInboundStream stream1 = new MockInboundStream(mockInput, chunkSize);
        stream1.pushDoneItem();
        inboundStreams.add(stream1);
        Tasklet tasklet = createTasklet();

        // When

        for (int i = 0; i < MOCK_INPUT_LENGTH; i += chunkSize) {
            assertEquals(MADE_PROGRESS, tasklet.call());
        }
        // Complete the inbound stream processing
        assertEquals(MADE_PROGRESS, tasklet.call());

        // Then

        assertEquals(DONE, tasklet.call());
        assertEquals(mockInput, consumer.getList());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void when_moreInputAvailable_then_consumeIt() throws Exception {
        // Given
        MockInboundStream stream1 = new MockInboundStream(mockInput, 2 * MOCK_INPUT_LENGTH);
        inboundStreams.add(stream1);
        Tasklet tasklet = createTasklet();
        assertEquals(MADE_PROGRESS, tasklet.call());

        // When
        stream1.push(10, 11);
        stream1.pushDoneItem();

        // Then

        // Process the new items
        assertEquals(MADE_PROGRESS, tasklet.call());
        // Complete the inbound stream processing
        assertEquals(MADE_PROGRESS, tasklet.call());
        // Complete overall processing
        assertEquals(DONE, tasklet.call());

        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), consumer.getList());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void when_dataNotAvailable_then_noProgress() throws Exception {
        // Given
        MockInboundStream stream1 = new MockInboundStream(mockInput, mockInput.size() / 2);
        stream1.pushDoneItem();
        inboundStreams.add(stream1);
        Tasklet tasklet = createTasklet();
        assertEquals(MADE_PROGRESS, tasklet.call());

        // When
        stream1.pause();

        // Then
        assertEquals(NO_PROGRESS, tasklet.call());
        assertFalse("isComplete", consumer.isComplete());
    }

    @Test
    public void when_twoInboundEdges_then_consumeBoth() throws Exception {
        // Given
        MockInboundStream stream1 = new MockInboundStream(0, mockInput, 1 + mockInput.size());
        MockInboundStream stream2 = new MockInboundStream(1, mockInput, 1 + mockInput.size());
        stream1.pushDoneItem();
        stream2.pushDoneItem();
        inboundStreams.add(stream1);
        inboundStreams.add(stream2);
        Tasklet tasklet = createTasklet();

        // When - Then

        // Exhaust edge stream 0
        assertEquals(MADE_PROGRESS, tasklet.call());
        assertTrue(stream1.isDone());
        // Complete edge stream 0
        tasklet.call();

        // Exhaust edge stream 1
        assertEquals(MADE_PROGRESS, tasklet.call());
        assertTrue(stream2.isDone());
        // Complete edge stream 1
        tasklet.call();

        assertEquals(2 * mockInput.size(), consumer.getList().size());
        // Complete overall processing
        assertTrue(tasklet.call().isDone());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_multipleInput_oneFinishedEarlier() throws Exception {
        MockInboundStream stream1 = new MockInboundStream(Arrays.asList(1, 2), 2);
        MockInboundStream stream2 = new MockInboundStream(mockInput, mockInput.size());
        inboundStreams.add(stream1);
        inboundStreams.add(stream2);
        Tasklet tasklet = createTasklet();

        assertEquals(MADE_PROGRESS, tasklet.call());
        assertEquals(MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(12, consumer.getList().size());
        assertTrue("isComplete", consumer.isComplete());
    }


    @Test
    public void testProgress_when_consumerYields() throws Exception {
        MockInboundStream stream1 = new MockInboundStream(mockInput, 10);
        inboundStreams.add(stream1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);

        assertEquals(MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1), consumer.getList());
        assertFalse("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_consumerYieldsOnSameItem() throws Exception {
        MockInboundStream stream1 = new MockInboundStream(mockInput, 10);
        inboundStreams.add(stream1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);
        assertEquals(MADE_PROGRESS, tasklet.call());
        consumer.yieldOn(2);
        assertEquals(NO_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1), consumer.getList());
        assertFalse("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_consumerYieldsAgain() throws Exception {
        MockInboundStream stream1 = new MockInboundStream(mockInput, 10);
        inboundStreams.add(stream1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);

        assertEquals(MADE_PROGRESS, tasklet.call());

        consumer.yieldOn(4);
        assertEquals(MADE_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(0, 1, 2, 3), consumer.getList());

        assertTrue(tasklet.call().isDone());

        assertEquals(mockInput, consumer.getList());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_consumerYieldsAndThenRuns() throws Exception {
        MockInboundStream stream1 = new MockInboundStream(mockInput, 10);
        inboundStreams.add(stream1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);

        assertEquals(MADE_PROGRESS, tasklet.call());
        assertTrue(tasklet.call().isDone());

        assertEquals(mockInput, consumer.getList());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void testProgress_when_consumerYieldsAndNoInput() throws Exception {
        MockInboundStream stream1 = new MockInboundStream(mockInput, 3);
        inboundStreams.add(stream1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);
        assertEquals(MADE_PROGRESS, tasklet.call());

        stream1.pause();

        assertEquals(MADE_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(0, 1, 2), consumer.getList());
        assertFalse("isComplete", consumer.isComplete());
    }

    @Test
    public void testIsBlocking() {
        inboundStreams.add(new MockInboundStream(mockInput, 10));
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

    private static void callUntilDone(Tasklet tasklet) throws Exception {
        int iterCount = 0;
        for (TaskletResult r; !(r = tasklet.call()).isDone();) {
            assertTrue(r.isMadeProgress());
            assertTrue("tasklet.call() invoked " + CALL_COUNT_LIMIT + " times without reaching completion",
                    ++iterCount < CALL_COUNT_LIMIT);
        }
    }
}
