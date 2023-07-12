/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CircularQueueTest {

    @Test(expected = IllegalArgumentException.class)
    public void test_construction_whenNegativeCapacity() {
        new CircularQueue<>(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_construction_whenZeroCapacity() {
        new CircularQueue<>(0);
    }

    @Test
    public void test_construction() {
        CircularQueue<Integer> queue = new CircularQueue<>(128);
        assertTrue(queue.isEmpty());
        assertFalse(queue.isFull());
        assertEquals(0, queue.size());
        assertEquals(128, queue.remaining());
        assertEquals(128, queue.capacity());
    }

    @Test
    public void test_construction_whenCapacityNotPowerOf2() {
        CircularQueue<Integer> queue = new CircularQueue<>(60);
        assertEquals(64, queue.capacity());
    }

    @Test
    public void test_offer() {
        CircularQueue<Integer> queue = new CircularQueue<>(128);
        for (int k = 0; k < queue.capacity() - 1; k++) {
            assertTrue(queue.offer(k));
            assertEquals(k + 1, queue.size());
            assertEquals(queue.capacity() - 1 - k, queue.remaining());
            assertFalse(queue.isEmpty());
            assertFalse(queue.isFull());
        }

        assertTrue(queue.offer(128));
        assertEquals(queue.capacity(), queue.size());
        assertEquals(0, queue.remaining());
        assertFalse(queue.isEmpty());
        assertTrue(queue.isFull());
    }

    @Test
    public void test_offer_whenFull() {
        CircularQueue<Integer> queue = new CircularQueue<>(128);
        for (int k = 0; k < queue.capacity(); k++) {
            queue.offer(k);
        }

        boolean offered = queue.offer(1234);
        assertFalse(offered);
        assertEquals(128, queue.capacity());
    }

    @Test(expected = NullPointerException.class)
    public void test_offer_whenNull() {
        CircularQueue<Integer> queue = new CircularQueue<>(128);
        queue.offer(null);
    }

    @Test
    public void test_peek() {
        CircularQueue<Integer> queue = new CircularQueue<>(128);
        assertNull(queue.peek());

        queue.offer(1);
        assertEquals(Integer.valueOf(1), queue.peek());
    }

    @Test
    public void test_add() {
        CircularQueue<Integer> queue = new CircularQueue<>(128);
        for (int k = 0; k < queue.capacity() - 1; k++) {
            queue.add(k);
            assertEquals(k + 1, queue.size());
            assertEquals(queue.capacity() - 1 - k, queue.remaining());
            assertFalse(queue.isEmpty());
            assertFalse(queue.isFull());
        }

        queue.add(128);
        assertEquals(queue.capacity(), queue.size());
        assertEquals(0, queue.remaining());
        assertFalse(queue.isEmpty());
        assertTrue(queue.isFull());
    }

    @Test
    public void test_add_whenFull() {
        CircularQueue<Integer> queue = new CircularQueue<>(128);
        for (int k = 0; k < queue.capacity(); k++) {
            queue.add(k);
        }

        assertThrows(IllegalStateException.class, () -> queue.add(1234));
    }

    @Test(expected = NullPointerException.class)
    public void test_add_whenNull() {
        CircularQueue<Integer> queue = new CircularQueue<>(128);
        queue.add(null);
    }

    @Test
    public void test_poll_whenEmpty() {
        CircularQueue<Integer> queue = new CircularQueue<>(128);
        Integer v = queue.poll();
        assertNull(v);

        assertEquals(0, queue.size());
        assertEquals(queue.capacity(), queue.remaining());
        assertTrue(queue.isEmpty());
        assertFalse(queue.isFull());
    }

    @Test
    public void test_poll() {
        CircularQueue<Integer> queue = new CircularQueue<>(4);
        queue.add(1);
        queue.add(2);
        queue.add(3);
        queue.add(4);

        assertEquals(Integer.valueOf(1), queue.poll());
        assertEquals(3, queue.size());
        assertEquals(1, queue.remaining());

        assertEquals(Integer.valueOf(2), queue.poll());
        assertEquals(2, queue.size());
        assertEquals(2, queue.remaining());

        assertEquals(Integer.valueOf(3), queue.poll());
        assertEquals(1, queue.size());
        assertEquals(3, queue.remaining());

        assertEquals(Integer.valueOf(4), queue.poll());
        assertEquals(0, queue.size());
        assertEquals(4, queue.remaining());
    }

    @Test
    public void test_drainFrom_whenEverythingFits() {
        CircularQueue<Integer> dst = new CircularQueue<>(256);
        Queue<Integer> src = new LinkedList<>();
        for (int k = 0; k < 128; k++) {
            src.add(k);
        }

        int result = dst.drainFrom(src);
        assertEquals(128, result);
        assertEquals(128, dst.size());
        assertTrue(src.isEmpty());
    }


    @Test
    public void test_drainFrom_whenSomeFit() {
        CircularQueue<Integer> dst = new CircularQueue<>(4);
        Queue<Integer> src = new LinkedList<>();
        src.add(1);
        src.add(2);
        src.add(3);
        src.add(4);
        src.add(5);

        int result = dst.drainFrom(src);

        assertEquals(4, result);
        assertEquals(4, dst.size());
        assertEquals(1, src.size());
        assertEquals(Integer.valueOf(1), dst.poll());
        assertEquals(Integer.valueOf(2), dst.poll());
        assertEquals(Integer.valueOf(3), dst.poll());
        assertEquals(Integer.valueOf(4), dst.poll());
    }

    @Test
    public void test_drainFrom_whenNoneFit() {
        CircularQueue<Integer> dst = new CircularQueue<>(4);
        dst.add(1);
        dst.add(2);
        dst.add(3);
        dst.add(4);

        Queue<Integer> src = new LinkedList<>();
        src.add(10);
        src.add(11);
        src.add(12);

        int result = dst.drainFrom(src);

        assertEquals(0, result);
        assertEquals(4, dst.size());
        assertEquals(3, src.size());
        assertEquals(Integer.valueOf(1), dst.poll());
        assertEquals(Integer.valueOf(2), dst.poll());
        assertEquals(Integer.valueOf(3), dst.poll());
        assertEquals(Integer.valueOf(4), dst.poll());
    }
}
