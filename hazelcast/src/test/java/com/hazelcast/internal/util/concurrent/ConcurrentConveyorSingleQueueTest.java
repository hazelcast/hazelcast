package com.hazelcast.internal.util.concurrent;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyorSingleQueue.concurrentConveyorSingleQueue;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConcurrentConveyorSingleQueueTest extends ConcurrentConveyorTest {

    private ConcurrentConveyorSingleQueue<Item> conveyorSingleQueue;

    @Before
    @Override
    public void before() {
        queueCount = 1;
        defaultQ = new OneToOneConcurrentArrayQueue<Item>(QUEUE_CAPACITY);
        conveyorSingleQueue = concurrentConveyorSingleQueue(doneItem, defaultQ);
        conveyor = conveyorSingleQueue;
    }

    @Test
    @Override
    public void when_drainQueue1ToList_then_listPopulated() {
        // FIXME
    }

    @Test
    @Override
    public void when_drainQueue1ToListLimited_then_listHasLimitedItems() {
        // FIXME
    }

    @Test
    @Override
    public void when_drainerFails_then_offerFailsWithItsFailureAsCause() {
        // FIXME
    }

    @Test
    @Override
    public void when_drainerDone_then_offerToFullQueueFails() {
        // FIXME
    }

    @Test
    public void when_offer_then_poll() {
        // when
        boolean didOffer = conveyorSingleQueue.offer(item1);

        // then
        assertTrue(didOffer);
        assertSame(item1, defaultQ.poll());
    }

    @Test
    public void when_submit_then_poll() {
        // when
        conveyorSingleQueue.submit(item2);

        // then
        assertSame(item2, defaultQ.poll());
    }
}
