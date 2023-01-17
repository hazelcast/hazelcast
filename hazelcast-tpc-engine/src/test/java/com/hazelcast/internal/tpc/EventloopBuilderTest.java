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
import static org.junit.Assert.assertThrows;

public abstract class EventloopBuilderTest {

    public abstract EventloopBuilder newBuilder();

    @Test
    public void test_setThreadFactory_whenNull() {
        EventloopBuilder builder = newBuilder();
        assertThrows(NullPointerException.class, ()->builder.setThreadFactory(null));
    }

    @Test
    public void test_setThreadFactory() {
        EventloopBuilder builder = newBuilder();

        Thread thread = new Thread();
        builder.setThreadFactory(r -> thread);

        Eventloop eventloop = builder.create();
        assertEquals(thread, eventloop.eventloopThread);
    }

    @Test
    public void test_setThreadNameSupplier() {
        EventloopBuilder builder = newBuilder();

        AtomicInteger id = new AtomicInteger();
        builder.setThreadNameSupplier(() -> "thethread" + id.incrementAndGet());

        Eventloop eventloop1 = builder.create();
        Eventloop eventloop2 = builder.create();
        assertEquals("thethread1", eventloop1.eventloopThread.getName());
        assertEquals("thethread2", eventloop2.eventloopThread.getName());
    }

    @Test
    public void test_setScheduledTaskQueueCapacity_whenZero() {
        EventloopBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setScheduledTaskQueueCapacity(0));
    }

    @Test
    public void test_setScheduledTaskQueueCapacity_whenNegative() {
        EventloopBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setScheduledTaskQueueCapacity(-1));
    }

    @Test
    public void test_setClockRefreshPeriod_whenZero() {
        EventloopBuilder builder = newBuilder();
        builder.setClockRefreshPeriod(0);
    }

    @Test
    public void test_setClockRefreshPeriod_whenNegative() {
        EventloopBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setClockRefreshPeriod(-1));
    }

    @Test
    public void test_setBatchSize_whenZero() {
        EventloopBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setBatchSize(0));
    }

    @Test
    public void test_setBatchSize_whenNegative() {
        EventloopBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setBatchSize(-1));
    }

    @Test
    public void test_setConcurrentRunQueueCapacity_whenZero() {
        EventloopBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setConcurrentTaskQueueCapacity(0));
    }

    @Test
    public void test_setConcurrentRunQueueCapacity_whenNegative() {
        EventloopBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setConcurrentTaskQueueCapacity(-1));
    }

    @Test
    public void test_setLocalRunQueueCapacity_whenZero() {
        EventloopBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setLocalTaskQueueCapacity(0));
    }

    @Test
    public void test_setLocalRunQueueCapacity_whenNegative() {
        EventloopBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setLocalTaskQueueCapacity(-1));
    }

    @Test
    public void test_setSchedulerSupplier_whenNull() {
        EventloopBuilder builder = newBuilder();
        assertThrows(NullPointerException.class, () -> builder.setSchedulerSupplier(null));
    }
}
