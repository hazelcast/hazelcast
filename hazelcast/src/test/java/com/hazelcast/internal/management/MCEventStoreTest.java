package com.hazelcast.internal.management;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

import static com.hazelcast.internal.management.ManagementCenterService.MCEventStore.MC_EVENTS_WINDOW_MILLIS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MCEventStoreTest {

    private static class FakeClock
            implements LongSupplier {

        long now = 42;

        @Override
        public long getAsLong() {
            return now;
        }
    }

    static final Address MC_1_REMOTE_ADDR;

    static final Address MC_2_REMOTE_ADDR;

    static final Address MC_3_REMOTE_ADDR;

    static {
        try {
            MC_1_REMOTE_ADDR = new Address("localhost", 5701);
            MC_2_REMOTE_ADDR = new Address("localhost", 5702);
            MC_3_REMOTE_ADDR = new Address("localhost", 5703);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private LinkedBlockingQueue<Event> queue;

    private ManagementCenterService.MCEventStore eventStore;

    private FakeClock clock;

    private void assertPolledEventCount(int expectedEventCount, Address mcRemoteAddress) {
        assertEquals(expectedEventCount, eventStore.pollMCEvents(mcRemoteAddress).size());
    }

    void inNextMilli(Runnable r) {
        clock.now++;
        r.run();
    }
    
    private void logEvent() {
        eventStore.log(new ManagementCenterServiceIntegrationTest.TestEvent(clock.now));
    }

    @Before
    public void before() {
        clock = new FakeClock();
        queue = new LinkedBlockingQueue<>();
        eventStore = new ManagementCenterService.MCEventStore(clock, queue);
    }

    @Test
    public void multipleMCs_canPollSeparately() {
        assertPolledEventCount(0, MC_2_REMOTE_ADDR);
        inNextMilli(() -> {
            logEvent();
            logEvent();
        });
        inNextMilli(() -> {
            assertPolledEventCount(2, MC_1_REMOTE_ADDR);
            assertPolledEventCount(2, MC_2_REMOTE_ADDR);
            assertPolledEventCount(0, MC_1_REMOTE_ADDR);
            assertPolledEventCount(0, MC_2_REMOTE_ADDR);
        });
        logEvent();
        inNextMilli(() -> {
            assertPolledEventCount(1, MC_1_REMOTE_ADDR);
        });
        inNextMilli(() -> {
            logEvent();
            assertPolledEventCount(1, MC_1_REMOTE_ADDR);
            assertPolledEventCount(2, MC_2_REMOTE_ADDR);
        });
        inNextMilli(() -> {
            logEvent();
            logEvent();
        });
        clock.now += MC_EVENTS_WINDOW_MILLIS;
        logEvent();
        inNextMilli(() -> {
            assertPolledEventCount(0, MC_1_REMOTE_ADDR);
            assertPolledEventCount(0, MC_2_REMOTE_ADDR);
        });
        inNextMilli(() -> {
            assertPolledEventCount(0, MC_1_REMOTE_ADDR);
        });
        inNextMilli(() -> {
            logEvent();
            assertPolledEventCount(1, MC_1_REMOTE_ADDR);
            assertPolledEventCount(1, MC_2_REMOTE_ADDR);
        });
    }

    @Test
    public void elemsReadByAllMCsAreCleared() {
        eventStore.pollMCEvents(MC_2_REMOTE_ADDR);
        inNextMilli(() -> {
            logEvent();
            logEvent();
        });
        clock.now += MC_EVENTS_WINDOW_MILLIS;
        inNextMilli(() -> {
            assertPolledEventCount(2, MC_1_REMOTE_ADDR);
            assertPolledEventCount(2, MC_2_REMOTE_ADDR);
            assertEquals(0, queue.size());
        });
    }
    
    @Test
    public void sameMilliEvent_reportedInNextPoll() {
        assertPolledEventCount(0, MC_1_REMOTE_ADDR);
        logEvent();
        inNextMilli(() -> {
            assertPolledEventCount(1, MC_1_REMOTE_ADDR);
            logEvent();
            logEvent();
        });
        inNextMilli(() -> {
            assertPolledEventCount(2, MC_1_REMOTE_ADDR);
        });
    }

    @Test
    public void disconnectRecognized_after30secInactivity() {
        inNextMilli(() -> {
            assertPolledEventCount(0, MC_1_REMOTE_ADDR);
            assertPolledEventCount(0, MC_2_REMOTE_ADDR);
            assertPolledEventCount(0, MC_3_REMOTE_ADDR);
        });
        logEvent();
        inNextMilli(() -> {
            assertPolledEventCount(1, MC_3_REMOTE_ADDR);
        });
        clock.now += TimeUnit.SECONDS.toMillis(15);
        inNextMilli(() ->{
            assertPolledEventCount(1, MC_1_REMOTE_ADDR);
        });
        inNextMilli(() -> {
            logEvent();
            logEvent();
        });
        clock.now += TimeUnit.SECONDS.toMillis(15);
        logEvent();
        logEvent();
        inNextMilli(() -> {
            assertPolledEventCount(4, MC_1_REMOTE_ADDR);
            // reads all 5 events, since its last-access TS is already cleared during previous read
            assertPolledEventCount(5, MC_3_REMOTE_ADDR);
        });
    }

    /**
     * Runs 50 threads in parallel, each thread performs 1000 tasks. Each task is one of:
     *  - logging 800 events
     *  - or polling as MC_1
     *  - or polling as MC_2
     *  
     * The test fails if any of the threads throw {@link java.util.ConcurrentModificationException} (or any other exception).  
     */
    @Test
    public void stressTest()
            throws InterruptedException {
        Runnable[] tasks = new Runnable[]{
                () -> {
                    for (int i = 0; i < 800; ++i) {
                        inNextMilli(() -> logEvent());
                    }
                },
                () -> inNextMilli(() -> eventStore.pollMCEvents(MC_1_REMOTE_ADDR)),
                () -> inNextMilli(() -> eventStore.pollMCEvents(MC_2_REMOTE_ADDR))
        };
        Random random = new Random();

        Set<Throwable> thrownByThreads = new ConcurrentSkipListSet<>();

        ThreadFactory tf = new ThreadFactoryBuilder()
                .setUncaughtExceptionHandler((thread, exc) -> {
                    exc.printStackTrace();
                    thrownByThreads.add(exc);
                })
                .build();
        
        int threadCount = 50, taskCount = 1000;
        List<Thread> threads = IntStream.range(0, threadCount)
                .mapToObj(i -> IntStream.range(0, taskCount)
                        .mapToObj(j -> tasks[Math.abs(random.nextInt()) % tasks.length])
                        .collect(toList()))
                .map(tasksForThread -> tf.newThread(() -> tasksForThread.forEach(Runnable::run)))
                .collect(toList());
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
        if (!thrownByThreads.isEmpty()) {
            thrownByThreads.forEach(Throwable::printStackTrace);
            throw new AssertionError("at least one thread threw an exception");
        }
    }
}
