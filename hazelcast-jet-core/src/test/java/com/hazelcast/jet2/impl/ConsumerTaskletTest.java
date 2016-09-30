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

import com.hazelcast.jet2.Consumer;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
public class ConsumerTaskletTest {

    private List<Integer> list;
    private Map<String, Input<? extends Integer>> inputMap;
    private ListConsumer<Integer> consumer;

    @Before
    public void setup() {
        this.list = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        this.consumer = new ListConsumer<>();
        this.inputMap = new HashMap<>();
    }

    @Test
    public void testConsumeSingleChunk_whenSingleInput() throws Exception {
        TestInput<Integer> input1 = new TestInput<>(4, list);
        input1.done();

        inputMap.put("input1", input1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1, 2, 3), consumer.getList());
        assertFalse("isComplete", consumer.isComplete());

    }

    @Test
    public void testConsumeAllChunks_whenSingleInput() throws Exception {
        TestInput<Integer> input1 = new TestInput<>(4, list);
        input1.done();

        inputMap.put("input1", input1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(list, consumer.getList());
        assertTrue("isComplete", consumer.isComplete());

    }

    @Test
    public void testConsume_whenSingleInputNotComplete() throws Exception {
        TestInput<Integer> input1 = new TestInput<>(list.size(), list);
        inputMap.put("input1", input1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(list, consumer.getList());
        assertFalse("isComplete", consumer.isComplete());

    }

    @Test
    public void testConsume_whenSingleInputNewData() throws Exception {
        TestInput<Integer> input1 = new TestInput<>(list.size(), list);
        inputMap.put("input1", input1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());

        input1.push(10, 11);
        input1.done();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), consumer.getList());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void testConsume_whenSingleInputNoProgress() throws Exception {
        TestInput<Integer> input1 = new TestInput<>(list.size(), list);
        inputMap.put("input1", input1);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.NO_PROGRESS, tasklet.call());
        assertFalse("isComplete", consumer.isComplete());
    }

    @Test
    public void testConsume_whenMultipleInput() throws Exception {
        TestInput<Integer> input1 = new TestInput<>(list.size(), list);
        TestInput<Integer> input2 = new TestInput<>(list.size(), list);
        input1.done();
        input2.done();
        inputMap.put("input1", input1);
        inputMap.put("input2", input2);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(list.size() * 2, consumer.getList().size());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void testConsume_whenMultipleInput_oneFinishedEarlier() throws Exception {
        TestInput<Integer> input1 = new TestInput<>(2, Arrays.asList(1, 2));
        TestInput<Integer> input2 = new TestInput<>(list.size(), list);
        input1.done();
        input2.done();
        inputMap.put("input1", input1);
        inputMap.put("input2", input2);
        Tasklet tasklet = createTasklet();

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(12, consumer.getList().size());
        assertTrue("isComplete", consumer.isComplete());
    }


    @Test
    public void testConsume_whenConsumerYields() throws Exception {
        TestInput<Integer> input1 = new TestInput<>(10, list);
        input1.done();
        inputMap.put("input1", input1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1), consumer.getList());
        assertFalse("isComplete", consumer.isComplete());
    }

    @Test
    public void testConsume_whenConsumerYieldsOnSameItem() throws Exception {
        TestInput<Integer> input1 = new TestInput<>(10, list);
        input1.done();
        inputMap.put("input1", input1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        consumer.yieldOn(2);
        assertEquals(TaskletResult.NO_PROGRESS, tasklet.call());
        assertEquals(Arrays.asList(0, 1), consumer.getList());
        assertFalse("isComplete", consumer.isComplete());
    }

    @Test
    public void testConsume_whenConsumerYieldsAgain() throws Exception {
        TestInput<Integer> input1 = new TestInput<>(10, list);
        input1.done();
        inputMap.put("input1", input1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());

        consumer.yieldOn(4);
        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());

        assertEquals(Arrays.asList(0, 1, 2, 3), consumer.getList());

        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(list, consumer.getList());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void testConsume_whenConsumerYieldsAndThenRuns() throws Exception {
        TestInput<Integer> input1 = new TestInput<>(10, list);
        input1.done();
        inputMap.put("input1", input1);
        Tasklet tasklet = createTasklet();

        consumer.yieldOn(2);

        assertEquals(TaskletResult.MADE_PROGRESS, tasklet.call());
        assertEquals(TaskletResult.DONE, tasklet.call());

        assertEquals(list, consumer.getList());
        assertTrue("isComplete", consumer.isComplete());
    }

    @Test
    public void testIsBlocking() {
        inputMap.put("input1", new TestInput<>(10, list));
        ConsumerTasklet<Integer> tasklet =
                new ConsumerTasklet<>(new Consumer<Integer>() {
            @Override
            public boolean consume(Integer object) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void complete() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isBlocking() {
                return true;
            }
        }, inputMap);
        assertTrue(tasklet.isBlocking());
    }

    private Tasklet createTasklet() {
        return new ConsumerTasklet<>(consumer, inputMap);
    }
}
