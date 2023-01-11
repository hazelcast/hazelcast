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

public abstract class EventloopFactoryTest {

    public abstract EventloopFactory create();

    @Test(expected = NullPointerException.class)
    public void test_setThreadFactory_whenNull() {
        EventloopFactory factory = create();
        factory.setThreadFactory(null);
    }

    @Test
    public void test_setThreadFactory() {
        EventloopFactory factory = create();

        Thread thread = new Thread();
        factory.setThreadFactory(r -> thread);

        Eventloop eventloop = factory.create();
        assertEquals(thread, eventloop.eventloopThread);
    }

    @Test
    public void test_setThreadNameSupplier() {
        EventloopFactory factory = create();

        AtomicInteger id = new AtomicInteger();
        factory.setThreadNameSupplier(() -> "thethread" + id.incrementAndGet());

        Eventloop eventloop1 = factory.create();
        Eventloop eventloop2 = factory.create();
        assertEquals("thethread1", eventloop1.eventloopThread.getName());
        assertEquals("thethread2", eventloop2.eventloopThread.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setScheduledTaskQueueCapacity_whenZero() {
        EventloopFactory factory = create();
        factory.setScheduledTaskQueueCapacity(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setScheduledTaskQueueCapacity_whenNegative() {
        EventloopFactory factory = create();
        factory.setScheduledTaskQueueCapacity(-1);
    }

    @Test
    public void test_setClockRefreshPeriod_whenZero() {
        EventloopFactory factory = create();
        factory.setClockRefreshPeriod(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setClockRefreshPeriod_whenNegative() {
        EventloopFactory factory = create();
        factory.setClockRefreshPeriod(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setBatchSize_whenZero() {
        EventloopFactory factory = create();
        factory.setBatchSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setBatchSize_whenNegative() {
        EventloopFactory factory = create();
        factory.setBatchSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setConcurrentRunQueueCapacity_whenZero() {
        EventloopFactory factory = create();
        factory.setConcurrentTaskQueueCapacity(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setConcurrentRunQueueCapacity_whenNegative() {
        EventloopFactory factory = create();
        factory.setConcurrentTaskQueueCapacity(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setLocalRunQueueCapacity_whenZero() {
        EventloopFactory factory = create();
        factory.setLocalTaskQueueCapacity(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setLocalRunQueueCapacity_whenNegative() {
        EventloopFactory factory = create();
        factory.setLocalTaskQueueCapacity(-1);
    }

    @Test(expected = NullPointerException.class)
    public void test_setSchedulerSupplier_whenNull() {
        EventloopFactory factory = create();
        factory.setSchedulerSupplier(null);
    }
}
