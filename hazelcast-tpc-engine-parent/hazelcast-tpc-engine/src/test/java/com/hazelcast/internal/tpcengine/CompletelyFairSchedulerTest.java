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

package com.hazelcast.internal.tpcengine;

import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.internal.tpcengine.CompletelyFairScheduler.niceToWeight;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class CompletelyFairSchedulerTest {

    public static final long TARGET_LATENCY_NANOS = 100;
    public static final long MIN_GRANULARITY_NANOS = 10;
    public static final int RUN_QUEUE_CAPACITY = 100;

    private CompletelyFairScheduler scheduler;

    @Before
    public void setup() {
        scheduler = new CompletelyFairScheduler(RUN_QUEUE_CAPACITY, TARGET_LATENCY_NANOS, MIN_GRANULARITY_NANOS);
    }

    @Test
    public void test_pickNext_whenEmpty() {
        assertNull(scheduler.pickNext());
        assertEquals(0, scheduler.runQueueSize());
    }

    @Test
    public void test_timeSliceNanosActive_sameWeight() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = niceToWeight(0);
        q1.virtualRuntimeNanos = 1000;
        scheduler.enqueue(q1);

        scheduler.pickNext();
        assertEquals(TARGET_LATENCY_NANOS, scheduler.timeSliceNanosActive());

        scheduler.yieldActive();

        TaskQueue q2 = new TaskQueue();
        q2.weight = niceToWeight(0);
        q2.virtualRuntimeNanos = 1000;
        scheduler.enqueue(q2);

        scheduler.pickNext();
        assertEquals(TARGET_LATENCY_NANOS / 2, scheduler.timeSliceNanosActive());
    }

//    @Test
//    public void test_timeSliceNanos_minGranularity() {
//        fail("not implemented yet");
//    }

    @Test
    public void test_timeSliceNanosActive_differentWeight() {
        TaskQueue q1 = new TaskQueue();
        q1.name = "q1";
        q1.weight = niceToWeight(0);
        q1.virtualRuntimeNanos = 1000;
        scheduler.enqueue(q1);

        TaskQueue q2 = new TaskQueue();
        q2.name = "q2";
        q2.weight = niceToWeight(1);
        q2.virtualRuntimeNanos = 1000;
        scheduler.enqueue(q2);

        assertSame(q1, scheduler.pickNext());
        // a task with a higher weight will get a larger timeslice.
        assertEquals(55, scheduler.timeSliceNanosActive());

        scheduler.updateActive(1000);
        scheduler.yieldActive();

        assertSame(q2, scheduler.pickNext());
        // a task with a lower weight will get a smaller timeslice.
        // there should roughly be 20% difference between the timeslice of q1 and q2
        assertEquals(44, scheduler.timeSliceNanosActive());
    }

    @Test
    public void test_pickNext() {
        TaskQueue q1 = new TaskQueue();
        q1.virtualRuntimeNanos = 1000;
        q1.weight = niceToWeight(0);
        scheduler.enqueue(q1);

        TaskQueue q2 = new TaskQueue();
        q2.virtualRuntimeNanos = 100;
        q2.weight = niceToWeight(0);
        scheduler.enqueue(q2);

        TaskQueue q3 = new TaskQueue();
        q3.virtualRuntimeNanos = 10000;
        q3.weight = niceToWeight(0);
        scheduler.enqueue(q3);
        assertEquals(3, scheduler.runQueueSize());

        assertEquals(q2, scheduler.pickNext());
        scheduler.dequeueActive();
        assertEquals(2, scheduler.runQueueSize());

        assertEquals(q1, scheduler.pickNext());
        scheduler.dequeueActive();
        assertEquals(1, scheduler.runQueueSize());

        assertEquals(q3, scheduler.pickNext());
        scheduler.dequeueActive();
        assertEquals(0, scheduler.runQueueSize());
    }

    @Test
    public void test_yieldActive() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = niceToWeight(0);
        q1.virtualRuntimeNanos = 1000;
        scheduler.enqueue(q1);

        TaskQueue q2 = new TaskQueue();
        q2.weight = niceToWeight(0);
        q2.virtualRuntimeNanos = 100;
        scheduler.enqueue(q2);

        TaskQueue q3 = new TaskQueue();
        q3.weight = niceToWeight(0);
        q3.virtualRuntimeNanos = 10000;
        scheduler.enqueue(q3);

        assertEquals(q2, scheduler.pickNext());
        scheduler.updateActive(100000);
        scheduler.yieldActive();
        assertEquals(3, scheduler.runQueueSize());

        assertEquals(q1, scheduler.pickNext());
        scheduler.updateActive(10);
        scheduler.yieldActive();
        assertEquals(3, scheduler.runQueueSize());

        assertEquals(q1, scheduler.pickNext());
        scheduler.updateActive(100000000);
        scheduler.yieldActive();
        assertEquals(3, scheduler.runQueueSize());

        assertEquals(q3, scheduler.pickNext());
    }

    @Test
    public void test_dequeueActive() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = niceToWeight(0);
        q1.virtualRuntimeNanos = 1;
        scheduler.enqueue(q1);

        TaskQueue q2 = new TaskQueue();
        q2.weight = niceToWeight(1);
        q2.virtualRuntimeNanos = 2;
        scheduler.enqueue(q2);

        TaskQueue q3 = new TaskQueue();
        q3.weight = niceToWeight(2);
        q3.virtualRuntimeNanos = 3;
        scheduler.enqueue(q3);

        assertEquals(q1, scheduler.pickNext());
        scheduler.dequeueActive();
        assertEquals(2, scheduler.runQueueSize());
        assertEquals(q2.weight + q3.weight, scheduler.totalWeight);

        assertEquals(q2, scheduler.pickNext());
        scheduler.dequeueActive();
        assertEquals(1, scheduler.runQueueSize());
        assertEquals(q3.weight, scheduler.totalWeight);

        assertEquals(q3, scheduler.pickNext());
        scheduler.dequeueActive();
        assertEquals(0, scheduler.runQueueSize());
        assertEquals(0, scheduler.totalWeight);
    }

    @Test
    public void test_enqueue() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = niceToWeight(0);

        TaskQueue q2 = new TaskQueue();
        q2.weight = niceToWeight(0);

        scheduler.enqueue(q1);
        assertEquals(1, scheduler.runQueueSize());
        assertEquals(q1.weight, scheduler.totalWeight);

        scheduler.enqueue(q2);
        assertEquals(2, scheduler.runQueueSize());
        assertEquals(q1.weight + q2.weight, scheduler.totalWeight);
    }

    @Test
    public void test_updateActive_when_nice_0() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = niceToWeight(0);

        scheduler.enqueue(q1);
        scheduler.pickNext();
        scheduler.updateActive(100000);

        assertEquals(100000, q1.actualRuntimeNanos);
        assertEquals(100000, q1.virtualRuntimeNanos);
    }

    @Test
    public void test_updateActive_when_nice_1() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = niceToWeight(1);

        scheduler.enqueue(q1);
        scheduler.pickNext();
        scheduler.updateActive(100000);

        assertEquals(100000, q1.actualRuntimeNanos);
        // the task gets a vruntime penalty due to its low weight.
        assertEquals(125030, q1.virtualRuntimeNanos);
    }

    @Test
    public void test_enqueue_vruntime_updated_whenFallenBehind() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = niceToWeight(0);

        int min_vruntime = 100000;
        scheduler.min_virtualRuntimeNanos = min_vruntime;

        scheduler.enqueue(q1);
        assertEquals(min_vruntime, q1.virtualRuntimeNanos);
    }

    @Test
    public void test_enqueue_vruntime_not_updated_whenAhead() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = niceToWeight(0);

        int min_vruntime = 100000;
        scheduler.min_virtualRuntimeNanos = min_vruntime;

        q1.virtualRuntimeNanos = 10 * min_vruntime;

        scheduler.enqueue(q1);
        assertEquals(10 * min_vruntime, q1.virtualRuntimeNanos);
    }
}
