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

package com.hazelcast.internal.util.concurrent;

import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.util.function.Consumer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractConcurrentArrayQueueTest {

    // must be a power of two to work
    static final int CAPACITY = 1 << 2;

    AbstractConcurrentArrayQueue<Integer> queue;

    private List<Integer> emptyList = emptyList();

    @Test
    public void testIsEmpty_whenEmpty() {
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        queue.offer(1);

        assertFalse(queue.isEmpty());
    }

    @Test
    public void testContains_whenContains() {
        queue.offer(23);

        assertTrue(queue.contains(23));
    }

    @Test
    public void testContains_whenNotContains() {
        assertFalse(queue.contains(42));
    }

    @Test
    public void testContains_whenNull() {
        assertFalse(queue.contains(null));
    }

    @Test
    public void testContainsAll_whenContainsAll() {
        queue.offer(1);
        queue.offer(23);
        queue.offer(42);
        queue.offer(95);

        assertTrue(queue.containsAll(asList(23, 42)));
    }

    @Test
    public void testContainsAll_whenNotContainsAll() {
        queue.offer(1);
        queue.offer(95);

        assertFalse(queue.containsAll(asList(23, 42)));
    }

    @Test
    public void testAddAll() {
        queue.addAll(asList(23, 42));

        assertEquals(2, queue.size());
        assertTrue(queue.contains(23));
        assertTrue(queue.contains(42));
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

    @Test
    public void testOffer() {
        assertTrue(queue.offer(1));
    }

    @Test
    public void testOffer_whenQueueIsFull_thenReject() {
        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(i);
        }

        assertFalse(queue.offer(23));
    }

    @Test
    public void testOffer_whenArrayQueueWasCompletelyFilled_thenUpdateHeadCache() {
        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(i);
        }
        queue.poll();

        assertTrue(queue.offer(23));
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void testOffer_whenNull_thenAssert() {
        queue.offer(null);
    }

    @Test
    public void testPoll() {
        queue.offer(23);
        queue.offer(42);

        int result1 = queue.poll();
        int result2 = queue.poll();

        assertEquals(23, result1);
        assertEquals(42, result2);
    }

    @Test
    public void testDrain() {
        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(i);
        }

        queue.drain(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                assertNotNull(integer);
            }
        });
    }

    @Test
    public void testDrainTo() {
        testDrainTo(3, 3);
    }

    @Test
    public void testDrainTo_whenLimitIsLargerThanQueue_thenDrainAllElements() {
        testDrainTo(CAPACITY + 1, CAPACITY);
    }

    @Test
    public void testDrainTo_whenLimitIsZero_thenDoNothing() {
        testDrainTo(0, 0);
    }

    @Test
    public void testDrainTo_whenLimitIsNegative_thenDoNothing() {
        testDrainTo(-1, 0);
    }

    private void testDrainTo(int limit, int expected) {
        List<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < CAPACITY; i++) {
            queue.offer(i);
        }

        queue.drainTo(result, limit);

        assertEquals(expected, result.size());
    }
}
