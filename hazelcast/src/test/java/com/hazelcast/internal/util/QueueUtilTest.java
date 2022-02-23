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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Predicate;

import static com.hazelcast.test.HazelcastTestSupport.assertUtilityConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueUtilTest {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(QueueUtil.class);
    }

    @Test
    public void drainQueueToZero() {
        Queue<Integer> queue = new LinkedList<Integer>();
        for (int i = 0; i < 100; i++) {
            queue.offer(i);
        }

        int drained = QueueUtil.drainQueue(queue);
        assertEquals(100, drained);
        assertTrue(queue.isEmpty());
    }

    @Test
    public void drainQueueToNonZero() {
        Queue<Integer> queue = new LinkedList<Integer>();
        for (int i = 0; i < 100; i++) {
            queue.offer(i);
        }

        int drained = QueueUtil.drainQueue(queue, 50, null);
        assertEquals(50, drained);
        assertEquals(50, queue.size());
    }

    @Test
    public void drainQueueToZeroWithPredicate() {
        Queue<Integer> queue = new LinkedList<Integer>();
        for (int i = 0; i < 100; i++) {
            queue.offer(i);
        }

        int drained = QueueUtil.drainQueue(queue, new Predicate<Integer>() {
            @Override
            public boolean test(Integer integer) {
                return integer % 2 == 0;
            }
        });
        assertEquals(50, drained);
        assertTrue(queue.isEmpty());
    }

    @Test
    public void drainQueueToNonZeroWithPredicate() {
        Queue<Integer> queue = new LinkedList<Integer>();
        for (int i = 0; i < 100; i++) {
            queue.offer(i);
        }

        int drained = QueueUtil.drainQueue(queue, 50, new Predicate<Integer>() {
            @Override
            public boolean test(Integer integer) {
                return integer % 2 == 0;
            }
        });
        assertEquals(25, drained);
        assertEquals(50, queue.size());
    }
}
