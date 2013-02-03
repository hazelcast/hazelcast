/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import static junit.framework.Assert.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class SimpleBoundedQueueTest {

    private SimpleBoundedQueue<String> queue = new SimpleBoundedQueue<String>(10);

    @After
    public void clear() {
        queue.clear();
    }

    @Test
    public void testAdd() {
        queue.add("hello");
        assertEquals("hello", queue.poll());
    }

    @Test
    public void testSize() {
        queue.add("hello");
        assertEquals(1, queue.size());
        queue.add("world");
        assertEquals(2, queue.size());
    }

    @Test
    public void testOffer() {
        queue.offer("hello");
        queue.offer("world");
        assertEquals("hello", queue.poll());
        assertEquals("world", queue.poll());
    }

    @Test
    public void testPeek() {
        queue.offer("hello");
        queue.add("world");
        assertEquals("hello", queue.peek());
    }

    @Test
    public void testPoll() {
        queue.offer("hello");
        queue.offer("hello");
        assertEquals("hello", queue.poll());
        assertEquals("hello", queue.poll());
    }

    @Test
    public void testCapacity() {
        queue.clear();
        assertEquals(0, queue.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(queue.offer(String.valueOf(i)));
            assertEquals(i + 1, queue.size());
        }
        for (int i = 0; i < 5; i++) {
            assertFalse(queue.offer(String.valueOf(i)));
            assertEquals(10, queue.size());
        }
        for (int i = 0; i < 5; i++) {
            assertEquals(String.valueOf(i), queue.poll());
            assertEquals(10 - i - 1, queue.size());
        }
        for (int i = 0; i < 5; i++) {
            assertTrue(queue.offer(String.valueOf(i)));
        }
        assertEquals(10, queue.size());
    }

    @Test
    public void testIterator() {
        // Unsupported
        /*queue.offer("hello");
        queue.offer("hello");
        for(String message: queue){
            assertEquals("hello", message);
        }*/
    }
}
