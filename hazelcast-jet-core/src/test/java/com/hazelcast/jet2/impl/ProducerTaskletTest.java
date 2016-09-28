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

import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
public class ProducerTaskletTest {

    private List<Integer> list;
    private ListProducer<Integer> producer;
    private HashMap<String, Output> outputMap;

    @Before
    public void setup() {
        this.list = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        this.producer = new ListProducer<>(list);
        this.outputMap = new HashMap<>();
    }

    @Test
    public void testProduce_whenSingleOutput() throws Exception {
        TestOutput<Object> output1 = new TestOutput<>(10);
        outputMap.put("output1", output1);
        Tasklet tasklet = createTasklet();

        TaskletResult result = tasklet.call();

        assertEquals(TaskletResult.DONE, result);
        assertEquals(list, output1.get());
    }

    @Test
    public void testProduce_whenMultipleOutputs() throws Exception {
        TestOutput<Object> output1 = new TestOutput<>(10);
        TestOutput<Object> output2 = new TestOutput<>(10);
        outputMap.put("output1", output1);
        outputMap.put("output2", output2);
        Tasklet tasklet = createTasklet();

        TaskletResult result = tasklet.call();

        assertEquals(TaskletResult.DONE, result);
        assertEquals(list, output1.get());
        assertEquals(list, output2.get());
    }

    @Test
    public void testProduce_whenOutputIsFull() throws Exception {
        TestOutput<Object> output1 = new TestOutput<>(5);
        outputMap.put("output1", output1);
        Tasklet tasklet = createTasklet();

        TaskletResult result = tasklet.call();

        assertEquals(TaskletResult.NOT_DONE, result);
    }

    @Test
    public void testProduce_whenBackoff() throws Exception {
        TestOutput<Object> output1 = new TestOutput<>(5);
        outputMap.put("output1", output1);
        Tasklet tasklet = createTasklet();

        TaskletResult result = tasklet.call();

        assertEquals(TaskletResult.NOT_DONE, result);

        result = tasklet.call();

        assertEquals(TaskletResult.NOT_DONE_BACKOFF, result);

        assertEquals(Arrays.asList(0, 1, 2, 3, 4), output1.get());
    }

    @Test
    public void testProduce_whenOutputFullThenDrained() throws Exception {
        TestOutput<Object> output1 = new TestOutput<>(5);
        outputMap.put("output1", output1);
        Tasklet tasklet = createTasklet();

        TaskletResult result = tasklet.call();

        assertEquals(TaskletResult.NOT_DONE, result);
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), output1.drain());

        result = tasklet.call();

        assertEquals(TaskletResult.DONE, result);

        assertEquals(Arrays.asList(5, 6, 7, 8, 9), output1.get());
    }

    @Test
    public void testProduce_whenOnlyOneOutputFull() throws Exception {
        TestOutput<Object> output1 = new TestOutput<>(10);
        TestOutput<Object> output2 = new TestOutput<>(5);
        outputMap.put("output1", output1);
        outputMap.put("output2", output2);
        Tasklet tasklet = createTasklet();

        TaskletResult result = tasklet.call();

        assertEquals(TaskletResult.NOT_DONE, result);
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5), output1.get());
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), output2.drain());

        result = tasklet.call();

        assertEquals(TaskletResult.DONE, result);

        assertEquals(list, output1.get());
        assertEquals(Arrays.asList(5, 6, 7, 8, 9), output2.get());
    }

    private ProducerTasklet createTasklet() {
        return new ProducerTasklet(producer, outputMap);
    }
}
