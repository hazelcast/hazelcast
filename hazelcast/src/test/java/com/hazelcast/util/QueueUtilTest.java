/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.Queue;

import static com.hazelcast.test.HazelcastTestSupport.assertUtilityConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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

        int drained = QueueUtil.drainQueue(queue, 50);
        assertEquals(50, drained);
        assertEquals(50, queue.size());
    }
}
