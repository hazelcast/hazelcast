package com.hazelcast.internal.util.concurrent;


import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.ExpectedException.none;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConcurrentConveyorTest {

    @Rule
    public ExpectedException excRule = none();

    private final int queueCapacity = 2;
    private final int queueCount = 2;
    private final Item item1 = new Item();
    private final Item item2 = new Item();
    private final List<Item> batch = new ArrayList<Item>(queueCapacity);

    private OneToOneConcurrentArrayQueue<Item> defaultQ;
    private final Item doneItem = new Item();
    private ConcurrentConveyor<Item> conveyor;

    @Before
    public void before() {
        final QueuedPipe<Item>[] qs = new QueuedPipe[queueCount];
        defaultQ = new OneToOneConcurrentArrayQueue<Item>(queueCapacity);
        qs[0] = defaultQ;
        qs[1] = new OneToOneConcurrentArrayQueue<Item>(queueCapacity);
        conveyor = concurrentConveyor(doneItem, qs);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mustPassSomeQueues() {
        concurrentConveyor(doneItem);
    }

    @Test
    public void submitterGoneItem() {
        assertSame(doneItem, conveyor.submitterGoneItem());
    }

    @Test
    public void queueCount() {
        assertEquals(queueCount, conveyor.queueCount());
    }

    @Test
    public void getQueueAtIndex() {
        assertSame(defaultQ, conveyor.queue(0));
    }

    @Test
    public void when_offerToQueueZero_then_poll() {
        // When
        final boolean didOffer = conveyor.offer(0, item1);

        // Then
        assertTrue(didOffer);
        assertSame(item1, defaultQ.poll());
    }

    @Test
    public void when_offerToGivenQueue_then_poll() {
        // When
        final boolean didOffer = conveyor.offer(defaultQ, item1);

        // Then
        assertTrue(didOffer);
        assertSame(item1, defaultQ.poll());
    }

    @Test
    public void when_submitToGivenQueue_then_poll() {
        // When
        conveyor.submit(defaultQ, item1);

        // Then
        assertSame(item1, defaultQ.poll());
    }

    @Test
    public void when_drainToList_then_listPopulated() {
        // Given
        conveyor.offer(0, item1);
        conveyor.offer(0, item2);

        // When
        conveyor.drainTo(batch);

        // Then
        assertEquals(asList(item1, item2), batch);
    }

    @Test
    public void when_drainQueue1ToList_then_listPopulated() {
        // Given
        conveyor.offer(1, item1);
        conveyor.offer(1, item2);

        // When
        conveyor.drainTo(1, batch);

        // Then
        assertEquals(asList(item1, item2), batch);
    }

    @Test
    public void when_drainQueue1ToListLimited_then_listHasLimitedItems() {
        // Given
        conveyor.offer(1, item1);
        conveyor.offer(1, item2);

        // When
        conveyor.drainTo(1, batch, 1);

        // Then
        assertEquals(singletonList(item1), batch);
    }

    @Test
    public void when_drainToListLimited_then_listHasLimitItems() {
        // Given
        conveyor.offer(0, item1);
        conveyor.offer(0, item2);

        // When
        conveyor.drainTo(batch, 1);

        // Then
        assertEquals(singletonList(item1), batch);
    }

    @Test
    public void when_drainerDone_then_offerToFullQueueFails() {
        // Given
        assertTrue(conveyor.offer(1, item1));
        assertTrue(conveyor.offer(1, item2));

        // When
        conveyor.drainerDone();

        // Then
        excRule.expect(ConcurrentConveyorException.class);
        conveyor.offer(1, item1);
    }

    @Test
    public void when_drainerDone_then_submitToFullQueueFails() {
        // Given
        assertTrue(conveyor.offer(defaultQ, item1));
        assertTrue(conveyor.offer(defaultQ, item2));

        // When
        conveyor.drainerDone();

        // Then
        excRule.expect(ConcurrentConveyorException.class);
        conveyor.submit(defaultQ, item1);
    }

    @Test
    public void when_interrupted_then_submitToFullQueueFails() {
        // Given
        conveyor.drainerArrived();
        assertTrue(conveyor.offer(defaultQ, item1));
        assertTrue(conveyor.offer(defaultQ, item2));

        // When
        currentThread().interrupt();

        // Then
        excRule.expect(ConcurrentConveyorException.class);
        conveyor.submit(defaultQ, item1);
    }

    @Test
    public void when_drainerLeavesThenArrives_then_offerDoesntFail() {
        // Given
        assertTrue(conveyor.offer(defaultQ, item1));
        assertTrue(conveyor.offer(defaultQ, item2));
        conveyor.drainerDone();

        // When
        conveyor.drainerArrived();

        // Then
        conveyor.offer(defaultQ, item1);
    }

    @Test
    public void when_drainerFails_then_offerFailsWithItsFailureAsCause() {
        // Given
        assertTrue(conveyor.offer(1, item1));
        assertTrue(conveyor.offer(1, item2));

        // When
        final Exception drainerFailure = new Exception("test failure");
        conveyor.drainerFailed(drainerFailure);

        // Then
        try {
            conveyor.offer(1, item1);
            fail("Expected exception not thrown");
        } catch (ConcurrentConveyorException e) {
            assertSame(drainerFailure, e.getCause());
        }
    }

    @Test(expected = NullPointerException.class)
    public void when_callDrainerFailedWithNull_then_throwNPE() {
        conveyor.drainerFailed(null);
    }

    @Test
    public void when_drainerDone_then_isDrainerGoneReturnsTrue() {
        // When
        conveyor.drainerDone();

        // Then
        assertTrue(conveyor.isDrainerGone());
    }

    @Test
    public void when_drainerFailed_then_isDrainerGoneReturnsTrue() {
        // When
        conveyor.drainerFailed(new Exception("test failure"));

        // Then
        assertTrue(conveyor.isDrainerGone());
    }

    @Test
    public void when_backpressureOn_then_submitBlocks() throws InterruptedException {
        // Given
        final AtomicBoolean flag = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread() { public void run() {
            // When
            conveyor.backpressureOn();
            latch.countDown();
            parkNanos(MILLISECONDS.toNanos(10));

            // Then
            assertFalse(flag.get());
            conveyor.backpressureOff();
        }}.start();

        latch.await();
        conveyor.submit(defaultQ, item1);
        flag.set(true);
    }

    @Test
    public void awaitDrainerGone_blocksUntilDrainerGone() throws InterruptedException {
        // Given
        final AtomicBoolean flag = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread() { public void run() {
            // When
            conveyor.drainerArrived();
            latch.countDown();
            parkNanos(MILLISECONDS.toNanos(10));

            // Then
            assertFalse(flag.get());
            conveyor.drainerDone();
        }}.start();

        latch.await();
        conveyor.awaitDrainerGone();
        flag.set(true);
    }

    private static class Item {
    }
}
