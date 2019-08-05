/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static com.hazelcast.util.scheduler.ScheduleType.FOR_EACH;
import static com.hazelcast.util.scheduler.ScheduleType.POSTPONE;
import static com.hazelcast.util.scheduler.SecondsBasedEntryTaskScheduler.findRelativeSecond;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Has timing sensitive tests, do not make a {@link ParallelJVMTest}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SecondsBasedEntryTaskSchedulerTest {

    @Mock
    private TaskScheduler taskScheduler = mock(TaskScheduler.class);

    @Mock
    @SuppressWarnings("unchecked")
    private ScheduledEntryProcessor<Integer, Integer> entryProcessor = mock(ScheduledEntryProcessor.class);

    private SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler;

    @Before
    @SuppressWarnings("unchecked")
    public void mockScheduleMethod() {
        when(taskScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(ScheduledFuture.class));
    }

    @Test
    public void test_scheduleEntry_postpone() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_rescheduleEntry_postpone() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertTrue(scheduler.schedule(10000, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test(timeout = 10000)
    public void test_doNotRescheduleEntryWithinSameSecond_postpone() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, POSTPONE);
        final int delayMillis = 0;
        final int key = 1;

        int startSecond;
        boolean firstResult;
        boolean secondResult;
        int stopSecond = 0;

        do {
            // startSecond must be greater than stopSecond on each trial round
            // to guarantee the first schedule attempt to be successful.
            while ((startSecond = findRelativeSecond(delayMillis)) == stopSecond) {
                sleepMillis(1);
            }

            // we can just assert the second result if the relative second is still the same,
            // otherwise we may create a false negative failure since the second could have passed after the schedule() call.
            firstResult = scheduler.schedule(delayMillis, key, 1);
            secondResult = scheduler.schedule(delayMillis, key, 1);
            stopSecond = findRelativeSecond(delayMillis);
        } while (startSecond != stopSecond);

        assertTrue("First schedule() call should always be successful", firstResult);
        assertFalse("Second schedule() call should not be successful within the same second", secondResult);
        assertNotNull(scheduler.get(key));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_cancelEntry_postpone() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.size());
        assertNotNull(scheduler.cancel(1));
        assertEquals(0, scheduler.size());
    }

    @Test
    public void test_scheduleEntry_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_scheduleEntryMultipleTimes_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertTrue(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(2, scheduler.size());
    }

    @Test
    public void test_cancelIfExists_postpone() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.cancelIfExists(1, 1));
    }

    @Test
    public void test_cancelIfExists_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.cancelIfExists(1, 1));
    }

    @Test
    public void test_cancelIfExistsWithInvalidValue_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(0, scheduler.cancelIfExists(1, 0));
    }

    @Test
    public void test_cancelIfExistsMultiple_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertTrue(scheduler.schedule(100, 1, 2));
        assertEquals(1, scheduler.cancelIfExists(1, 1));
    }

    @Test
    public void test_cancelAll() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertTrue(scheduler.schedule(100, 1, 2));
        scheduler.cancelAll();
        assertEquals(0, scheduler.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_executeScheduledEntry() {
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(taskScheduler.schedule(runnableCaptor.capture(), anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(ScheduledFuture.class));

        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.size());

        Runnable runnable = runnableCaptor.getValue();
        assertNotNull(runnable);
        runnable.run();
        assertEquals(0, scheduler.size());
    }

    @Test
    public void test_toString() {
        scheduler = new SecondsBasedEntryTaskScheduler<Integer, Integer>(taskScheduler, entryProcessor, FOR_EACH);

        assertNotNull(scheduler.toString());
    }
}
