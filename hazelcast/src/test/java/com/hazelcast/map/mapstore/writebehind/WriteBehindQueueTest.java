package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.mapstore.writebehind.WriteBehindQueues.createDefaultWriteBehindQueue;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindQueueTest extends HazelcastTestSupport {

    @Test
    public void smoke() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();

        assertEquals(0, queue.size());
    }

    @Test
    public void testOffer() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();

        fillQueue(queue, 1000);

        assertEquals(1000, queue.size());
    }

    @Test(expected = ReachedMaxSizeException.class)
    public void testWBQMaxSizeException() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();
        // put total 1001 items. Max allowed is 1000.
        fillQueue(queue, 1001);
    }

    @Test(expected = ReachedMaxSizeException.class)
    public void testWBQMaxSizeException_withMultipleWBQ() {
        final AtomicInteger counter = new AtomicInteger(0);
        final WriteBehindQueue<DelayedEntry> queue1 = createWBQ(counter);
        final WriteBehindQueue<DelayedEntry> queue2 = createWBQ(counter);
        final WriteBehindQueue<DelayedEntry> queue3 = createWBQ(counter);
        final WriteBehindQueue<DelayedEntry> queue4 = createWBQ(counter);
        // put total 1001 items. Max allowed is 1000.
        fillQueue(queue1, 10);
        fillQueue(queue2, 500);
        fillQueue(queue3, 400);
        fillQueue(queue4, 91);
    }

    @Test
    public void testWBQ_counter_is_zero() {
        final AtomicInteger counter = new AtomicInteger(0);
        final WriteBehindQueue<DelayedEntry> queue = createWBQ(counter);
        fillQueue(queue, 100);
        queue.clear();

        assertEquals(0, counter.intValue());
    }


    @Test
    public void testOffer_thenRemove_thenOffer() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();
        fillQueue(queue, 1000);

        queue.removeAll();

        fillQueue(queue, 1000);

        assertEquals(1000, queue.size());
    }

    @Test
    public void testCounter_offer_thenRemove() {
        final AtomicInteger counter = new AtomicInteger(0);
        final WriteBehindQueue<DelayedEntry> queue = createWBQ(counter);
        fillQueue(queue, 1000);
        queue.removeAll();

        assertEquals(0, counter.intValue());
    }

    @Test
    public void testRemoveEmpty() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();

        queue.removeFirst();

        assertEquals(0, queue.size());
    }

    @Test
    public void testClear() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();

        queue.clear();

        assertEquals(0, queue.size());
    }


    @Test
    public void testClearFull() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();

        fillQueue(queue, 1000);

        queue.clear();

        assertEquals(0, queue.size());
    }


    @Test
    public void testRemoveAll() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();

        fillQueue(queue, 1000);

        queue.removeAll();

        assertEquals(0, queue.size());
    }

    private void fillQueue(WriteBehindQueue queue, int numberOfItems) {
        SerializationService ss1 = new SerializationServiceBuilder().build();
        final long storeTime = Clock.currentTimeMillis();
        for (int i = 0; i < numberOfItems; i++) {
            final DelayedEntry<Data, Object> e = DelayedEntry.createWithNullValue(ss1.toData(i), storeTime, i);
            queue.offer(e);
        }
    }

    private WriteBehindQueue createWBQ() {
        final AtomicInteger counter = new AtomicInteger(0);
        return createWBQ(counter);
    }

    private WriteBehindQueue createWBQ(AtomicInteger counter) {
        final int maxSizePerNode = 1000;
        return createDefaultWriteBehindQueue(maxSizePerNode, counter);
    }

}
