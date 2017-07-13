/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.scheduler;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.scheduler.ScheduleType.FOR_EACH;
import static com.hazelcast.util.scheduler.ScheduleType.POSTPONE;
import static com.hazelcast.util.scheduler.ScheduleType.SCHEDULE_IF_NEW;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SlotBasedEntryTaskSchedulerTest {

    @Mock
    private ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);

    @Mock
    private ScheduledEntryProcessor<Integer, Integer> entryProcessor = mock(ScheduledEntryProcessor.class);

    @After
    public void after() {
        System.clearProperty("com.hazelcast.clock.impl");
    }

    @Test
    public void testCeilToUnit() {
        SlotBasedEntryTaskScheduler scheduler = new SlotBasedEntryTaskScheduler(null, null, null);
        assertEquals(10, scheduler.ceilToUnit(500));
        assertEquals(10, scheduler.ceilToUnit(499));
        assertEquals(1, scheduler.ceilToUnit(1));
        assertEquals(0, scheduler.ceilToUnit(0));
    }

    @Test
    public void test_scheduleEntry_scheduleIfNew() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_scheduleEntryOnlyOnce_scheduleIfNew() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertFalse(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_cancelEntry_scheduleIfNew() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.size());
        assertNotNull(scheduler.cancel(1));
        assertEquals(0, scheduler.size());
    }

    @Test
    public void test_cancelEntry_notExistingKey() {
        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertNull(scheduler.cancel(1));
    }

    @Test
    public void test_scheduleEntry_postpone() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_rescheduleEntry_postpone() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertTrue(scheduler.schedule(10000, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_dontRescheduleEntryWithinSameSlot_postpone() {
        System.setProperty("com.hazelcast.clock.impl", "com.hazelcast.util.StaticClock");
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(0, 1, 1));
        assertFalse(scheduler.schedule(0, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_cancelEntry_postpone() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.size());
        assertNotNull(scheduler.cancel(1));
        assertEquals(0, scheduler.size());
    }

    @Test
    public void test_scheduleEntry_foreach() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_scheduleEntryMultipleTimes_foreach() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertTrue(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(2, scheduler.size());
    }

    @Test
    public void test_cancelIfExists_scheduleIfNew() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.cancelIfExists(1, 1));
    }

    @Test
    public void test_cancelIfExists_postpone() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.cancelIfExists(1, 1));
    }

    @Test
    public void test_cancelIfExists_foreach() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.cancelIfExists(1, 1));
    }

    @Test
    public void test_cancelIfExistsWithInvalidValue_foreach() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(0, scheduler.cancelIfExists(1, 0));
    }

    @Test
    public void test_cancelIfExistsMultiple_foreach() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertTrue(scheduler.schedule(100, 1, 2));
        assertEquals(1, scheduler.cancelIfExists(1, 1));
    }

    @Test
    public void test_cancelIfExists_notExistingKey() {
        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertEquals(0, scheduler.cancelIfExists(1, 0));
    }

    @Test
    public void test_cancelAll() {
        mockScheduleMethod();

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertTrue(scheduler.schedule(100, 1, 2));
        scheduler.cancelAll();
        assertEquals(0, scheduler.size());
    }

    @Test
    public void test_executeScheduledEntry() {
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(executorService.schedule(runnableCaptor.capture(), anyLong(), any(TimeUnit.class))).thenReturn(
                mock(ScheduledFuture.class));

        final SlotBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.size());

        final Runnable runnable = runnableCaptor.getValue();
        assertNotNull(runnable);
        runnable.run();
        assertEquals(0, scheduler.size());
    }

    @Test
    public void test_toString() {
        assertNotNull(new SlotBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH).toString());
    }

    private void mockScheduleMethod() {
        when(executorService.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(
                mock(ScheduledFuture.class));
    }

}