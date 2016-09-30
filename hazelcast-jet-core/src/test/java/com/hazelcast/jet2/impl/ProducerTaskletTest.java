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
import com.hazelcast.jet2.Producer;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class ProducerTaskletTest {

    private List<Integer> list;
    private ListProducer<Integer> producer;
    private Map<String, Output<Integer>> outputMap;

    @Before
    public void setup() {
        this.list = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        this.producer = new ListProducer<>(list);
        producer.setBatchSize(4);
        this.outputMap = new HashMap<>();
    }

    @Test
    public void testSingleChunk_whenSingleOutput() throws Exception {
        TestOutput<Integer> output1 = new TestOutput<>(10);
        outputMap.put("output1", output1);
        Tasklet tasklet = createTasklet();
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1, 2, 3), output1.getBuffer());
    }

    @Test
    public void testSingleChunk_whenMultipleOutputs() throws Exception {
        TestOutput<Integer> output1 = new TestOutput<>(10);
        TestOutput<Integer> output2 = new TestOutput<>(10);
        outputMap.put("output1", output1);
        outputMap.put("output2", output2);

        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1, 2, 3), output1.getBuffer());
        assertEquals(Arrays.asList(0, 1, 2, 3), output2.getBuffer());
    }

    @Test
    public void testAllChunks_whenSingleOutput() throws Exception {
        TestOutput<Integer> output1 = new TestOutput<>(10);
        outputMap.put("output1", output1);

        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(list, output1.getBuffer());
    }

    @Test
    public void testAllChunks_whenMultipleOutputs() throws Exception {
        TestOutput<Integer> output1 = new TestOutput<>(10);
        TestOutput<Integer> output2 = new TestOutput<>(10);
        outputMap.put("output1", output1);
        outputMap.put("output2", output2);

        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(list, output1.getBuffer());
        assertEquals(list, output2.getBuffer());
    }

    @Test
    public void testProgress_whenOutputIsFull() throws Exception {
        TestOutput<Integer> output1 = new TestOutput<>(4);
        outputMap.put("output1", output1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.NO_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(0, 1, 2, 3), output1.drain());

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.NO_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(4, 5, 6, 7), output1.drain());

        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(Arrays.asList(8, 9), output1.drain());
    }

    @Test
    public void testProgress_whenOutputFullThenFullyDrained() throws Exception {
        TestOutput<Integer> output1 = new TestOutput<>(1);
        outputMap.put("output1", output1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.NO_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0), output1.drain());

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.NO_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(1), output1.getBuffer());
    }

    @Test
    public void testProgress_whenOnlyOneOutputFull() throws Exception {
        TestOutput<Integer> output1 = new TestOutput<>(2);
        TestOutput<Integer> output2 = new TestOutput<>(4);
        outputMap.put("output1", output1);
        outputMap.put("output2", output2);
        Tasklet tasklet = createTasklet();


        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.NO_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(0, 1), output1.getBuffer());
        assertEquals(Arrays.asList(0, 1), output2.getBuffer());
    }

    @Test
    public void testNoProgress_whenProducerIdle() throws Exception {
        TestOutput<Integer> output1 = new TestOutput<>(10);
        outputMap.put("output1", output1);
        Tasklet tasklet = createTasklet();
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1, 2, 3), output1.drain());

        producer.pause();

        assertEquals(TaskletResult.NO_PROGRESS, tasklet.call());

        producer.resume();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(4, 5, 6, 7), output1.drain());
    }

    @Test
    public void testDone_whenProducerIdleAndComplete() throws Exception {
        TestOutput<Integer> output1 = new TestOutput<>(10);
        outputMap.put("output1", output1);
        Tasklet tasklet = createTasklet();
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1, 2, 3), output1.drain());

        producer.completeEarly();

        assertEquals(TaskletResult.DONE, tasklet.call());

        assertTrue(output1.drain().isEmpty());
    }

    @Test
    public void testProgress_whenProducerIdleButPendingOutput() throws Exception {
        TestOutput<Integer> output1 = new TestOutput<>(2);
        outputMap.put("output1", output1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1), output1.drain());
        producer.pause();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(2, 3), output1.drain());

    }

    @Test
    public void testIsBlocking() {
        outputMap.put("input1", new TestOutput<>(10));
        ProducerTasklet<Integer> tasklet =
                new ProducerTasklet<>(new Producer<Integer>() {
                    @Override
                    public boolean produce(OutputCollector<? super Integer> collector) {
                        return false;
                    }

                    @Override
                    public boolean isBlocking() {
                        return true;
                    }
                }, outputMap);
        assertTrue(tasklet.isBlocking());
    }

    private ProducerTasklet createTasklet() {
        return new ProducerTasklet(producer, outputMap);
    }
}
