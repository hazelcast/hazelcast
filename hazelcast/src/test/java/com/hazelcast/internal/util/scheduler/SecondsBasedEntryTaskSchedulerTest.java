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

package com.hazelcast.internal.util.scheduler;

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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.scheduler.ScheduleType.FOR_EACH;
import static com.hazelcast.internal.util.scheduler.ScheduleType.POSTPONE;
import static com.hazelcast.internal.util.scheduler.SecondsBasedEntryTaskScheduler.findRelativeSecond;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Has timing sensitive tests, do not make a {@link ParallelJVMTest}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("ConstantConditions")
public class SecondsBasedEntryTaskSchedulerTest {

    @Mock
    private TaskScheduler taskScheduler = mock(TaskScheduler.class);

    @Mock
    @SuppressWarnings("unchecked")
    private ScheduledEntryProcessor<String, String> entryProcessor = mock(ScheduledEntryProcessor.class);

    private SecondsBasedEntryTaskScheduler<String, String> scheduler;

    @Before
    @SuppressWarnings("unchecked")
    public void mockScheduleMethod() {
        when(taskScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(ScheduledFuture.class));
    }

    @Test
    public void test_scheduleEntry_postpone() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, "k", "v"));
        assertEquals(1, scheduler.size());
        assertEquals("v", scheduler.get("k").getValue());
    }

    @Test
    public void test_rescheduleEntry_postpone() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(1000, "k", "x"));
        assertTrue(scheduler.schedule(7000, "k", "y"));
        assertEquals(1, scheduler.size());
        assertEquals("y", scheduler.get("k").getValue());

        // discovered in reverse engineering that instead of postponing one can advance, this may be not intentional:
        assertTrue(scheduler.schedule(4000, "k", "z"));
        assertEquals(1, scheduler.size());
        assertEquals("z", scheduler.get("k").getValue());
    }

    @Test(timeout = 10000)
    public void test_doNotRescheduleEntryWithinSameSecond_postpone() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, POSTPONE);
        final int delayMillis = 0;
        final String key = "k";

        int startSecond;
        boolean firstResult;
        boolean secondResult;
        int stopSecond;

        do {
            scheduler.cancel(key); // clean up after previous iteration to guarantee the first schedule attempt to be successful
            startSecond = findRelativeSecond(delayMillis);
            firstResult = scheduler.schedule(delayMillis, key, "x");
            secondResult = scheduler.schedule(delayMillis, key, "y");
            stopSecond = findRelativeSecond(delayMillis);
        } while (startSecond != stopSecond); // make sure that both schedules were in the same relative second

        assertTrue("First schedule() call should always be successful", firstResult);
        assertFalse("Second schedule() call should not be successful within the same second", secondResult);
        assertEquals(1, scheduler.size());
        assertEquals("x", scheduler.get(key).getValue());
    }

    @Test
    public void test_scheduleEntry_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, "k", "v"));
        assertEquals(1, scheduler.size());
        assertEquals("v", scheduler.get("k").getValue());
    }

    @Test
    public void test_scheduleEntryMultipleTimes_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(1100, "k", "x"));
        assertTrue(scheduler.schedule(1200, "k", "y"));
        assertTrue(scheduler.schedule(3000, "k", "z"));
        assertEquals(3, scheduler.size());
        ScheduledEntry<String, String> scheduledSample = scheduler.get("k"); // unspecified which one will be returned
        assertThat(scheduledSample.getValue(), isIn(asList("x", "y", "z")));
    }

    @Test
    public void test_cancelEntry_postpone() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, "k", "v"));
        assertNotNull(scheduler.cancel("k"));
        assertTrue(scheduler.isEmpty());
    }

    @Test
    public void test_cancelEntry_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, "k", "x"));
        assertTrue(scheduler.schedule(100, "k", "y"));
        assertNotNull(scheduler.cancel("k"));
        assertTrue(scheduler.isEmpty());
    }

    @Test
    public void test_cancelIfExists_postpone() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, "k", "v"));
        assertEquals(1, scheduler.cancelIfExists("k", "v"));
        assertTrue(scheduler.isEmpty());
    }

    @Test
    public void test_cancelIfExists_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, "k", "v"));
        assertEquals(1, scheduler.cancelIfExists("k", "v"));
        assertTrue(scheduler.isEmpty());
    }

    @Test
    public void test_cancelIfExistsWithInvalidValue_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, "k", "x"));
        assertEquals(0, scheduler.cancelIfExists("k", "y"));
        assertEquals("x", scheduler.get("k").getValue());
    }

    @Test
    public void test_cancelIfExistsMultiple_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, "k", "x"));
        assertTrue(scheduler.schedule(100, "k", "y"));
        assertEquals(1, scheduler.cancelIfExists("k", "y"));
        assertEquals("x", scheduler.get("k").getValue());
    }

    @Test
    public void test_cancelAll() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, "k", "x"));
        assertTrue(scheduler.schedule(100, "k", "y"));
        scheduler.cancelAll();
        assertTrue(scheduler.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_executeScheduledEntries_postpone() {
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(taskScheduler.schedule(runnableCaptor.capture(), anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(ScheduledFuture.class));

        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, POSTPONE);
        scheduler.schedule(1100, "k", "x");
        scheduler.schedule(1200, "k", "y");
        scheduler.schedule(3000, "k", "z");
        scheduler.schedule(3000, "j", "v");

        // simulate that the time's up and everything has been executed
        runnableCaptor.getAllValues().forEach(Runnable::run);
        assertTrue(scheduler.isEmpty());

        ArgumentCaptor<Collection<ScheduledEntry<String, String>>> entriesCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(entryProcessor, atLeastOnce()).process(same(scheduler), entriesCaptor.capture());

        List<String> entriesPassedToProcessor = entriesCaptor.getAllValues().stream()
                .flatMap(Collection::stream)
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(toList());

        assertThat(entriesPassedToProcessor, contains("k:z", "j:v"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_executeScheduledEntries_foreach() {
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        when(taskScheduler.schedule(runnableCaptor.capture(), anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(ScheduledFuture.class));

        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, FOR_EACH);
        scheduler.schedule(1100, "k", "x");
        scheduler.schedule(1200, "k", "y");
        scheduler.schedule(3000, "k", "z");
        scheduler.schedule(3000, "j", "v");

        // simulate that the time's up and everything has been executed
        runnableCaptor.getAllValues().forEach(Runnable::run);
        assertTrue(scheduler.isEmpty());

        ArgumentCaptor<Collection<ScheduledEntry<String, String>>> entriesCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(entryProcessor, atLeastOnce()).process(same(scheduler), entriesCaptor.capture());

        List<String> entriesPassedToProcessor = entriesCaptor.getAllValues().stream()
                .flatMap(Collection::stream)
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(toList());

        assertThat(entriesPassedToProcessor, contains("k:x", "k:y", "k:z", "j:v"));
    }

    @Test
    public void test_noExceptionsOnNonExistingKey_postpone() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, "k", "v"));
        assertNull(scheduler.get("j"));
        assertNull(scheduler.cancel("j"));
        assertEquals(0, scheduler.cancelIfExists("j", "v"));
        assertEquals("v", scheduler.get("k").getValue());
    }

    @Test
    public void test_noExceptionsOnNonExistingKey_foreach() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, "k", "v"));
        assertNull(scheduler.get("j"));
        assertNull(scheduler.cancel("j"));
        assertEquals(0, scheduler.cancelIfExists("j", "v"));
        assertEquals("v", scheduler.get("k").getValue());
    }

    @Test
    public void test_toString() {
        scheduler = new SecondsBasedEntryTaskScheduler<>(taskScheduler, entryProcessor, FOR_EACH);

        assertNotNull(scheduler.toString());
    }
}
