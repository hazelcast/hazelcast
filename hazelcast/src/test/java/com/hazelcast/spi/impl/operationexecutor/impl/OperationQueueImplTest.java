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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationQueueImplTest extends HazelcastTestSupport {

    private OperationQueueImpl operationQueue;
    private ArrayBlockingQueue<Object> normalQueue;
    private ArrayBlockingQueue<Object> priorityQueue;

    @Before
    public void setup() {
        normalQueue = new ArrayBlockingQueue<Object>(100);
        priorityQueue = new ArrayBlockingQueue<Object>(100);
        operationQueue = new OperationQueueImpl(normalQueue, priorityQueue);
    }

    // ================== add =====================

    @Test(expected = NullPointerException.class)
    public void add_whenNull() {
        operationQueue.add(null, false);
    }

    @Test
    public void add_whenPriority() throws InterruptedException {
        Object task = new Object();
        operationQueue.add(task, true);

        assertEquals(1, operationQueue.prioritySize());
        assertEquals(1, operationQueue.normalSize());
        assertEquals(2, operationQueue.size());
        assertEquals(1, priorityQueue.size());
        assertEquals(1, normalQueue.size());

        assertSame(task, priorityQueue.iterator().next());
        assertSame(OperationQueueImpl.TRIGGER_TASK, normalQueue.iterator().next());
    }

    @Test
    public void add_whenNormal() {
        Object task = new Object();
        operationQueue.add(task, false);

        assertContent(normalQueue, task);
        assertEmpty(priorityQueue);
        assertEquals(0, operationQueue.prioritySize());
        assertEquals(1, operationQueue.normalSize());
        assertEquals(1, operationQueue.size());
    }

    // ================== take =====================

    @Test
    public void take_whenPriorityItemAvailable() throws InterruptedException {
        Object task1 = "task1";
        Object task2 = "task2";
        Object task3 = "task3";

        operationQueue.add(task1, true);
        operationQueue.add(task2, true);
        operationQueue.add(task3, true);

        assertSame(task1, operationQueue.take(false));
        assertSame(task2, operationQueue.take(false));
        assertSame(task3, operationQueue.take(false));

        assertEquals(3, operationQueue.size());
        assertEquals(0, operationQueue.prioritySize());
        assertEquals(3, operationQueue.normalSize());
    }

    /**
     * It could be that in the low priority query there are a bunch of useless trigger tasks preceding a regular tasks.
     */
    @Test
    public void take_whenLowPriority_andManyPrecedingTriggerTasks() throws InterruptedException {
        Object task1 = "task1";
        Object task2 = "task2";
        Object task3 = "task3";
        Object task4 = "task4";

        operationQueue.add(task1, true);
        operationQueue.add(task2, true);
        operationQueue.add(task3, true);
        operationQueue.add(task4, false);

        assertSame(task1, operationQueue.take(false));
        assertSame(task2, operationQueue.take(false));
        assertSame(task3, operationQueue.take(false));

        // at this moment there are 3 trigger tasks and 1 normal task
        assertEquals(4, operationQueue.size());

        // when we take the item
        assertSame(task4, operationQueue.take(false));

        // all the trigger tasks are drained
        assertEquals(0, operationQueue.size());
        assertEquals(0, operationQueue.prioritySize());
        assertEquals(0, operationQueue.normalSize());
    }

    @Test
    public void take_whenRegularItemAvailable() throws InterruptedException {
        Object task1 = "task1";
        Object task2 = "task2";
        Object task3 = "task3";

        operationQueue.add(task1, false);
        operationQueue.add(task2, false);
        operationQueue.add(task3, false);

        assertSame(task1, operationQueue.take(false));
        assertSame(task2, operationQueue.take(false));
        assertSame(task3, operationQueue.take(false));

        assertEquals(0, operationQueue.size());
        assertEquals(0, operationQueue.prioritySize());
        assertEquals(0, operationQueue.normalSize());
    }

    @Test
    public void take_whenPriorityAndRegularItemAvailable() throws InterruptedException {
        Object task1 = "task1";
        Object task2 = "task2";
        Object task3 = "task3";

        operationQueue.add(task1, true);
        operationQueue.add(task2, false);
        operationQueue.add(task3, true);

        assertSame(task1, operationQueue.take(true));
        assertSame(task3, operationQueue.take(true));

        assertEquals(0, operationQueue.prioritySize());
    }

    @Test
    public void take_whenPriority_andNoItemAvailable_thenBlockTillItemAvailable() throws InterruptedException {
        final Object task1 = "task1";
        final Object task2 = "task2";

        operationQueue.add(task1, false);

        spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(4);
                operationQueue.add(task2, true);
            }
        });

        assertSame(task2, operationQueue.take(true));
        assertEquals(0, operationQueue.prioritySize());
    }

    @Test
    public void take_priorityIsRetrievedFirst() throws InterruptedException {
        Object priorityTask1 = "priority1";
        Object priorityTask2 = "priority2";
        Object priorityTask3 = "priority4";

        Object normalTask1 = "normalTask1";
        Object normalTask2 = "normalTask2";
        Object normalTask3 = "normalTask3";

        operationQueue.add(priorityTask1, true);
        operationQueue.add(normalTask1, false);
        operationQueue.add(normalTask2, false);

        operationQueue.add(priorityTask2, true);
        operationQueue.add(normalTask3, false);
        operationQueue.add(priorityTask3, true);

        assertSame(priorityTask1, operationQueue.take(false));
        assertSame(priorityTask2, operationQueue.take(false));
        assertSame(priorityTask3, operationQueue.take(false));
        assertSame(normalTask1, operationQueue.take(false));
        assertSame(normalTask2, operationQueue.take(false));
        assertSame(normalTask3, operationQueue.take(false));

        assertEmpty(priorityQueue);
        //assertContent(normalQueue, OperationQueueImpl.TRIGGER_TASK);
    }

    public void assertEmpty(Queue<Object> q) {
        assertEquals("expecting an empty operationQueue, but the operationQueue is:" + q, 0, q.size());
    }

    public void assertContent(Queue<Object> q, Object... expected) {
        List<Object> actual = new LinkedList<Object>(q);
        assertEquals(Arrays.asList(expected), actual);
    }
}
