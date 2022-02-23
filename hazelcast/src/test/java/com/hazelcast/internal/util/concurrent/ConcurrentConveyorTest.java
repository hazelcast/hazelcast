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

package com.hazelcast.internal.util.concurrent;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConcurrentConveyorTest {

    static final int QUEUE_CAPACITY = 2;

    @Rule
    public ExpectedException excRule = none();

    final Item doneItem = new Item();
    final Item item1 = new Item();
    final Item item2 = new Item();

    int queueCount;
    OneToOneConcurrentArrayQueue<Item> defaultQ;
    ConcurrentConveyor<Item> conveyor;

    private final List<Item> batch = new ArrayList<Item>(QUEUE_CAPACITY);

    @Before
    public void before() {
        queueCount = 2;
        defaultQ = new OneToOneConcurrentArrayQueue<Item>(QUEUE_CAPACITY);

        QueuedPipe<Item>[] qs = new QueuedPipe[queueCount];
        qs[0] = defaultQ;
        qs[1] = new OneToOneConcurrentArrayQueue<Item>(QUEUE_CAPACITY);
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
        // when
        boolean didOffer = conveyor.offer(0, item1);

        // then
        assertTrue(didOffer);
        assertSame(item1, defaultQ.poll());
    }

    @Test
    public void when_offerToGivenQueue_then_poll() {
        // when
        boolean didOffer = conveyor.offer(defaultQ, item1);

        // then
        assertTrue(didOffer);
        assertSame(item1, defaultQ.poll());
    }

    @Test
    public void when_submitToGivenQueue_then_poll() {
        // when
        conveyor.submit(defaultQ, item1);

        // then
        assertSame(item1, defaultQ.poll());
    }

    @Test
    public void when_drainToList_then_listPopulated() {
        // given
        conveyor.offer(0, item1);
        conveyor.offer(0, item2);

        // when
        conveyor.drainTo(batch);

        // then
        assertEquals(asList(item1, item2), batch);
    }

    @Test
    public void when_drainQueue1ToList_then_listPopulated() {
        // given
        conveyor.offer(1, item1);
        conveyor.offer(1, item2);

        // when
        conveyor.drainTo(1, batch);

        // then
        assertEquals(asList(item1, item2), batch);
    }

    @Test
    public void when_drainQueue1ToListLimited_then_listHasLimitedItems() {
        // given
        conveyor.offer(1, item1);
        conveyor.offer(1, item2);

        // when
        conveyor.drainTo(1, batch, 1);

        // then
        assertEquals(singletonList(item1), batch);
    }

    @Test
    public void when_drainToListLimited_then_listHasLimitItems() {
        // given
        conveyor.offer(0, item1);
        conveyor.offer(0, item2);

        // when
        conveyor.drainTo(batch, 1);

        // then
        assertEquals(singletonList(item1), batch);
    }

    @Test
    public void when_drainerDone_then_offerToFullQueueFails() {
        // given
        assertTrue(conveyor.offer(1, item1));
        assertTrue(conveyor.offer(1, item2));

        // when
        conveyor.drainerDone();

        // then
        excRule.expect(ConcurrentConveyorException.class);
        conveyor.offer(1, item1);
    }

    @Test
    public void when_drainerDone_then_submitToFullQueueFails() {
        // given
        assertTrue(conveyor.offer(defaultQ, item1));
        assertTrue(conveyor.offer(defaultQ, item2));

        // when
        conveyor.drainerDone();

        // then
        excRule.expect(ConcurrentConveyorException.class);
        conveyor.submit(defaultQ, item1);
    }

    @Test
    public void when_interrupted_then_submitToFullQueueFails() {
        // given
        conveyor.drainerArrived();
        assertTrue(conveyor.offer(defaultQ, item1));
        assertTrue(conveyor.offer(defaultQ, item2));

        // when
        currentThread().interrupt();

        // then
        excRule.expect(ConcurrentConveyorException.class);
        conveyor.submit(defaultQ, item1);
    }

    @Test
    public void when_drainerLeavesThenArrives_then_offerDoesntFail() {
        // given
        assertTrue(conveyor.offer(defaultQ, item1));
        assertTrue(conveyor.offer(defaultQ, item2));
        conveyor.drainerDone();

        // when
        conveyor.drainerArrived();

        // then
        conveyor.offer(defaultQ, item1);
    }

    @Test
    public void when_drainerFails_then_offerFailsWithItsFailureAsCause() {
        // given
        assertTrue(conveyor.offer(1, item1));
        assertTrue(conveyor.offer(1, item2));

        // when
        Exception drainerFailure = new Exception("test failure");
        conveyor.drainerFailed(drainerFailure);

        // then
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
        // when
        conveyor.drainerDone();

        // then
        assertTrue(conveyor.isDrainerGone());
    }

    @Test
    public void when_drainerFailed_then_isDrainerGoneReturnsTrue() {
        // when
        conveyor.drainerFailed(new Exception("test failure"));

        // then
        assertTrue(conveyor.isDrainerGone());
    }

    @Test
    public void when_backpressureOn_then_submitBlocks() throws InterruptedException {
        // given
        final AtomicBoolean flag = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread() {
            public void run() {
                // when
                conveyor.backpressureOn();
                latch.countDown();
                parkNanos(MILLISECONDS.toNanos(10));

                // then
                assertFalse(flag.get());
                conveyor.backpressureOff();
            }
        }.start();

        latch.await();
        conveyor.submit(defaultQ, item1);
        flag.set(true);
    }

    @Test
    public void awaitDrainerGone_blocksUntilDrainerGone() throws InterruptedException {
        // given
        final AtomicBoolean flag = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);

        new Thread() {
            public void run() {
                // when
                conveyor.drainerArrived();
                latch.countDown();
                parkNanos(MILLISECONDS.toNanos(10));

                // then
                assertFalse(flag.get());
                conveyor.drainerDone();
            }
        }.start();

        latch.await();
        conveyor.awaitDrainerGone();
        flag.set(true);
    }

    static class Item {
    }
}
