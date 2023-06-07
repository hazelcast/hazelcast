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

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CfsTaskQueueSchedulerTest {

    public static final long TARGET_LATENCY_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    public static final long MIN_GRANULARITY_NANOS = TimeUnit.MICROSECONDS.toNanos(100);

    private CfsTaskQueueScheduler scheduler;

    @Before
    public void setup() {
        scheduler = new CfsTaskQueueScheduler(100, TARGET_LATENCY_NANOS, MIN_GRANULARITY_NANOS);
    }

    @Test
    public void test_pickNext_whenEmpty() {
        assertNull(scheduler.pickNext());
    }

    @Test
    public void test_timeSliceNanosActive() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = 1;
        q1.virtualRuntimeNanos = 1000;
        scheduler.enqueue(q1);

        scheduler.pickNext();
        assertEquals(TARGET_LATENCY_NANOS, scheduler.timeSliceNanosActive());

        scheduler.yieldActive();

        TaskQueue q2 = new TaskQueue();
        q2.weight = 1;
        q2.virtualRuntimeNanos = 1000;
        scheduler.enqueue(q2);

        scheduler.pickNext();
        assertEquals(TARGET_LATENCY_NANOS / 2, scheduler.timeSliceNanosActive());
    }

    @Test
    public void test_pickNext() {
        TaskQueue q1 = new TaskQueue();
        q1.virtualRuntimeNanos = 1000;
        scheduler.enqueue(q1);

        TaskQueue q2 = new TaskQueue();
        q2.virtualRuntimeNanos = 100;
        scheduler.enqueue(q2);

        TaskQueue q3 = new TaskQueue();
        q3.virtualRuntimeNanos = 10000;
        scheduler.enqueue(q3);

        assertEquals(q2, scheduler.pickNext());
        scheduler.dequeueActive();

        assertEquals(q1, scheduler.pickNext());
        scheduler.dequeueActive();

        assertEquals(q3, scheduler.pickNext());
        scheduler.dequeueActive();
    }

    @Test
    public void test_yieldActive() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = 1;
        q1.virtualRuntimeNanos = 1000;
        scheduler.enqueue(q1);

        TaskQueue q2 = new TaskQueue();
        q2.weight = 1;
        q2.virtualRuntimeNanos = 100;
        scheduler.enqueue(q2);

        TaskQueue q3 = new TaskQueue();
        q3.weight = 1;
        q3.virtualRuntimeNanos = 10000;
        scheduler.enqueue(q3);

        assertEquals(q2, scheduler.pickNext());
        scheduler.updateActive(100000);
        scheduler.yieldActive();
        assertEquals(3, scheduler.nrRunning);

        assertEquals(q1, scheduler.pickNext());
        scheduler.updateActive(10);
        scheduler.yieldActive();
        assertEquals(3, scheduler.nrRunning);

        assertEquals(q1, scheduler.pickNext());
        scheduler.updateActive(100000000);
        scheduler.yieldActive();
        assertEquals(3, scheduler.nrRunning);

        assertEquals(q3, scheduler.pickNext());
    }


    @Test
    public void test_dequeueActive() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = 1;
        q1.virtualRuntimeNanos = 1;
        scheduler.enqueue(q1);

        TaskQueue q2 = new TaskQueue();
        q2.weight = 2;
        q2.virtualRuntimeNanos = 2;
        scheduler.enqueue(q2);

        TaskQueue q3 = new TaskQueue();
        q3.weight = 3;
        q3.virtualRuntimeNanos = 3;
        scheduler.enqueue(q3);

        assertEquals(q1, scheduler.pickNext());
        scheduler.dequeueActive();
        assertEquals(2, scheduler.nrRunning);
        assertEquals(q2.weight + q3.weight, scheduler.totalWeight);

        assertEquals(q2, scheduler.pickNext());
        scheduler.dequeueActive();
        assertEquals(1, scheduler.nrRunning);
        assertEquals(q3.weight, scheduler.totalWeight);

        assertEquals(q3, scheduler.pickNext());
        scheduler.dequeueActive();
        assertEquals(0, scheduler.nrRunning);
        assertEquals(0, scheduler.totalWeight);
    }

    @Test
    public void test_enqueue() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = 1;

        TaskQueue q2 = new TaskQueue();
        q2.weight = 2;

        scheduler.enqueue(q1);
        assertEquals(1, scheduler.nrRunning);
        assertEquals(1, scheduler.totalWeight);

        scheduler.enqueue(q2);
        assertEquals(2, scheduler.nrRunning);
        assertEquals(3, scheduler.totalWeight);
    }

    @Test
    public void test_enqueue_vruntime_updated_whenFallenBehind() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = 1;

        int min_vruntime = 100000;
        scheduler.min_virtualRuntimeNanos = min_vruntime;

        scheduler.enqueue(q1);
        assertEquals(min_vruntime, q1.virtualRuntimeNanos);
    }

    @Test
    public void test_enqueue_vruntime_not_updated_whenAhead() {
        TaskQueue q1 = new TaskQueue();
        q1.weight = 1;

        int min_vruntime = 100000;
        scheduler.min_virtualRuntimeNanos = min_vruntime;

        q1.virtualRuntimeNanos = 10 * min_vruntime;

        scheduler.enqueue(q1);
        assertEquals(10 * min_vruntime, q1.virtualRuntimeNanos);
    }
}
