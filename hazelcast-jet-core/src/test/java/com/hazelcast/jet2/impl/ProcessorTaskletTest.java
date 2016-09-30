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

import com.hazelcast.jet2.OutputCollector;
import com.hazelcast.jet2.Processor;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class ProcessorTaskletTest {

    private List<Integer> list;
    private Map<String, QueueHead<? extends Integer>> inputMap;
    private Map<String, QueueTail<? super Integer>> outputMap;
    private TestProcessor<Integer> processor;


    @Before
    public void setUp() throws Exception {
        this.list = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        this.processor = new TestProcessor<>();
        this.inputMap = new HashMap<>();
        this.outputMap = new HashMap<>();
    }

    @Test
    public void testSingleChunk_when_singleOutput() throws Exception {
        TestQueueHead<Integer> input1 = new TestQueueHead<>(10, list);
        TestQueueTail<Integer> output1 = new TestQueueTail<>(10);

        inputMap.put("input1", input1);
        outputMap.put("output1", output1);

        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(list, output1.getBuffer());
        assertEquals("input1", processor.lastInput);
    }

    @Test
    public void testSingleChunk_when_multipleOutputs() throws Exception {
        TestQueueHead<Integer> input1 = new TestQueueHead<>(10, list);
        TestQueueTail<Integer> output1 = new TestQueueTail<>(10);
        TestQueueTail<Integer> output2 = new TestQueueTail<>(10);

        inputMap.put("input1", input1);
        outputMap.put("output1", output1);
        outputMap.put("output2", output2);

        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(list, output1.getBuffer());
        assertEquals(list, output2.getBuffer());
        assertEquals("input1", processor.lastInput);
    }

    @Test
    public void testProgress_when_multipleChunks() throws Exception {
        TestQueueHead<Integer> input1 = new TestQueueHead<>(4, list);
        TestQueueTail<Integer> output1 = new TestQueueTail<>(10);

        inputMap.put("input1", input1);
        outputMap.put("output1", output1);

        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(list, output1.getBuffer());
        assertEquals("input1", processor.lastInput);
    }

    @Test
    public void testProgress_when_multipleInputs() throws Exception {
        TestQueueHead<Integer> input1 = new TestQueueHead<>(4, Arrays.asList(0, 1, 2, 3));
        TestQueueHead<Integer> input2 = new TestQueueHead<>(4, Arrays.asList(4, 5, 6, 7));
        TestQueueHead<Integer> input3 = new TestQueueHead<>(4, Arrays.asList(8, 9));
        TestQueueTail<Integer> output1 = new TestQueueTail<>(10);

        inputMap.put("input1", input1);
        inputMap.put("input2", input2);
        inputMap.put("input3", input3);
        outputMap.put("output1", output1);

        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(new HashSet<>(list), new HashSet<>(output1.getBuffer()));
    }

    @Test
    public void testProgress_when_pendingInputAndOutputEmpty() throws Exception {
        TestQueueHead<Integer> input1 = new TestQueueHead<>(4, list);
        TestQueueTail<Integer> output1 = new TestQueueTail<>(10);

        inputMap.put("input1", input1);
        outputMap.put("output1", output1);

        Tasklet tasklet = createTasklet();

        processor.paused = true;

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());

        assertTrue("isEmpty", output1.getBuffer().isEmpty());
    }

    private Tasklet createTasklet() {
        return new ProcessorTasklet<>(processor, inputMap, outputMap);
    }

    private static class TestProcessor<T> implements Processor<T, T> {

        private String lastInput;
        private boolean paused;
        private boolean produceIfPaused;

        @Override
        public boolean process(String input, T item, OutputCollector<? super T> collector) {
            if (paused) {
                if (produceIfPaused) {
                    collector.collect(item);
                }
                return false;
            }
            lastInput = input;
            collector.collect(item);
            return true;
        }

        @Override
        public boolean complete(OutputCollector<? super T> collector) {
            return true;
        }
    }
}
