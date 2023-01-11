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

package com.hazelcast.internal.tpc;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public abstract class EventloopBuilderTest {

    public abstract EventloopBuilder create();

    @Test(expected = NullPointerException.class)
    public void test_setThreadFactory_whenNull() {
        EventloopBuilder factory = create();
        factory.setThreadFactory(null);
    }

    @Test
    public void test_setThreadFactory() {
        EventloopBuilder factory = create();

        Thread thread = new Thread();
        factory.setThreadFactory(r -> thread);

        Eventloop eventloop = factory.create();
        assertEquals(thread, eventloop.eventloopThread);
    }

    @Test
    public void test_setThreadNameSupplier() {
        EventloopBuilder factory = create();

        AtomicInteger id = new AtomicInteger();
        factory.setThreadNameSupplier(() -> "thethread" + id.incrementAndGet());

        Eventloop eventloop1 = factory.create();
        Eventloop eventloop2 = factory.create();
        assertEquals("thethread1", eventloop1.eventloopThread.getName());
        assertEquals("thethread2", eventloop2.eventloopThread.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setScheduledTaskQueueCapacity_whenZero() {
        EventloopBuilder factory = create();
        factory.setScheduledTaskQueueCapacity(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setScheduledTaskQueueCapacity_whenNegative() {
        EventloopBuilder factory = create();
        factory.setScheduledTaskQueueCapacity(-1);
    }

    @Test
    public void test_setClockRefreshPeriod_whenZero() {
        EventloopBuilder factory = create();
        factory.setClockRefreshPeriod(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setClockRefreshPeriod_whenNegative() {
        EventloopBuilder factory = create();
        factory.setClockRefreshPeriod(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setBatchSize_whenZero() {
        EventloopBuilder factory = create();
        factory.setBatchSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setBatchSize_whenNegative() {
        EventloopBuilder factory = create();
        factory.setBatchSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setConcurrentRunQueueCapacity_whenZero() {
        EventloopBuilder factory = create();
        factory.setConcurrentTaskQueueCapacity(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setConcurrentRunQueueCapacity_whenNegative() {
        EventloopBuilder factory = create();
        factory.setConcurrentTaskQueueCapacity(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setLocalRunQueueCapacity_whenZero() {
        EventloopBuilder factory = create();
        factory.setLocalTaskQueueCapacity(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setLocalRunQueueCapacity_whenNegative() {
        EventloopBuilder factory = create();
        factory.setLocalTaskQueueCapacity(-1);
    }

    @Test(expected = NullPointerException.class)
    public void test_setSchedulerSupplier_whenNull() {
        EventloopBuilder factory = create();
        factory.setSchedulerSupplier(null);
    }
}
