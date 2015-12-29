package com.hazelcast.util.scheduler;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Before;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SecondsBasedEntryTaskSchedulerTest {

    @Mock
    private ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);

    @Mock
    private ScheduledEntryProcessor<Integer, Integer> entryProcessor = mock(ScheduledEntryProcessor.class);

    @Before
    public void mockScheduleMethod() {
        when(executorService.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(
                mock(ScheduledFuture.class));
    }

    @Test
    public void test_scheduleEntry_scheduleIfNew() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_scheduleEntryOnlyOnce_scheduleIfNew() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertFalse(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_cancelEntry_scheduleIfNew() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.size());
        assertNotNull(scheduler.cancel(1));
        assertEquals(0, scheduler.size());
    }

    @Test
    public void test_cancelEntry_notExistingKey() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertNull(scheduler.cancel(1));
    }

    @Test
    public void test_scheduleEntry_postpone() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_rescheduleEntry_postpone() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertTrue(scheduler.schedule(10000, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_dontRescheduleEntryWithinSameSecond_postpone() {
        System.setProperty(Clock.HAZELCAST_CLOCK_IMPL, StaticClock.class.getName());
        try {
            final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                    new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, POSTPONE);

            assertTrue(scheduler.schedule(0, 1, 1));
            assertFalse(scheduler.schedule(0, 1, 1));
            assertNotNull(scheduler.get(1));
            assertEquals(1, scheduler.size());
        } finally {
            System.clearProperty(Clock.HAZELCAST_CLOCK_IMPL);
        }
    }

    @Test
    public void test_cancelEntry_postpone() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.size());
        assertNotNull(scheduler.cancel(1));
        assertEquals(0, scheduler.size());
    }

    @Test
    public void test_scheduleEntry_foreach() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(1, scheduler.size());
    }

    @Test
    public void test_scheduleEntryMultipleTimes_foreach() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertTrue(scheduler.schedule(100, 1, 1));
        assertNotNull(scheduler.get(1));
        assertEquals(2, scheduler.size());
    }

    @Test
    public void test_cancelIfExists_scheduleIfNew() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.cancelIfExists(1, 1));
    }

    @Test
    public void test_cancelIfExists_postpone() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, POSTPONE);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.cancelIfExists(1, 1));
    }

    @Test
    public void test_cancelIfExists_foreach() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.cancelIfExists(1, 1));
    }

    @Test
    public void test_cancelIfExistsWithInvalidValue_foreach() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(0, scheduler.cancelIfExists(1, 0));
    }

    @Test
    public void test_cancelIfExistsMultiple_foreach() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertTrue(scheduler.schedule(100, 1, 2));
        assertEquals(1, scheduler.cancelIfExists(1, 1));
    }

    @Test
    public void test_cancelIfExists_notExistingKey() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertEquals(0, scheduler.cancelIfExists(1, 0));
    }

    @Test
    public void test_cancelAll() {
        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH);

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

        final SecondsBasedEntryTaskScheduler<Integer, Integer> scheduler =
                new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, SCHEDULE_IF_NEW);

        assertTrue(scheduler.schedule(100, 1, 1));
        assertEquals(1, scheduler.size());

        final Runnable runnable = runnableCaptor.getValue();
        assertNotNull(runnable);
        runnable.run();
        assertEquals(0, scheduler.size());
    }

    @Test
    public void test_toString() {
        assertNotNull(new SecondsBasedEntryTaskScheduler<Integer, Integer>(executorService, entryProcessor, FOR_EACH).toString());
    }
}
