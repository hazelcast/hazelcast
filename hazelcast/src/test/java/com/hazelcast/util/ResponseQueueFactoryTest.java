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

package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ResponseQueueFactoryTest extends HazelcastTestSupport {

    private BlockingQueue<Object> queue;

    @Before
    public void setUp() {
        queue = ResponseQueueFactory.newResponseQueue();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ResponseQueueFactory.class);
    }

    private List<Object> emptyList = emptyList();

    @Test
    public void testIsEmpty_whenEmpty() {
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        queue.offer(1);

        assertFalse(queue.isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIterator() {
        queue.iterator();
    }

    @Test(expected = IllegalStateException.class)
    public void testAddAll_whenQueueIsFull_thenThrowException() {
        queue.addAll(asList(23, 42));
    }

    @Test(expected = IllegalStateException.class)
    public void testAddAll_whenOverCapacity_thenThrowException() {
        queue.addAll(asList(1, 2, 3, 4, 5, 6));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveAll() {
        queue.removeAll(emptyList);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRetainAll() {
        queue.retainAll(emptyList);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemainingCapacity() {
        queue.remainingCapacity();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDrainTo() {
        queue.drainTo(emptyList);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDrainTo_withMaxElements() {
        queue.drainTo(emptyList, 1);
    }

    @Test
    public void testClear() {
        assertTrue(queue.offer(1));

        queue.clear();

        assertEquals(0, queue.size());
    }

    @Test
    public void testOffer() {
        assertTrue(queue.offer(1));
    }

    @Test
    public void testOffer_withTimeout() throws Exception {
        assertTrue(queue.offer(1, 2, TimeUnit.SECONDS));
    }

    @Test
    public void testPut() throws Exception {
        queue.put(23);

        assertEquals(1, queue.size());
    }

    @Test
    public void testPoll() {
        assertTrue(queue.offer(23));
        assertFalse(queue.offer(42));

        Object result1 = queue.poll();
        Object result2 = queue.poll();

        assertEquals(23, result1);
        assertNull(result2);
    }

    @Test
    public void testPoll_withTimeout() throws Exception {
        Thread thread = new Thread() {
            @Override
            public void run() {
                sleepSeconds(1);
                assertTrue(queue.offer(23));
            }
        };
        thread.start();

        assertEquals(23, queue.poll(10, TimeUnit.SECONDS));
        thread.join();
    }

    @Test
    public void testPoll_withTimeout_withoutResponse() throws Exception {
        assertNull(queue.poll(100, TimeUnit.MILLISECONDS));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPoll_withInvalidTimeout() throws Exception {
        queue.poll(-1, TimeUnit.SECONDS);
    }

    @Test
    public void testPeek() {
        assertTrue(queue.offer(23));

        assertEquals(23, queue.peek());
        assertEquals(1, queue.size());
    }

    @Test
    public void testTake() throws Exception {
        Thread thread = new Thread() {
            @Override
            public void run() {
                sleepSeconds(1);
                assertTrue(queue.offer(23));
            }
        };
        thread.start();

        assertEquals(23, queue.take());
        thread.join();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testNullHandling() {
        queue.offer(null);

        assertNull(queue.peek());
        assertNull(queue.poll());
    }
}
