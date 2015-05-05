package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.map.impl.mapstore.writebehind.DelayedEntry;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.impl.mapstore.writebehind.DelayedEntry.createWithNullValue;
import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createBoundedWriteBehindQueue;
import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueues.createDefaultWriteBehindQueue;
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
    public void testAddEnd() {
        WriteBehindQueue<DelayedEntry> queue = createWBQ();
        addEnd(1000, queue);

        assertEquals(1000, queue.size());
    }

    @Test
    public void testAddFront() {
        WriteBehindQueue<DelayedEntry> queue = createWBQ();
        List<DelayedEntry> delayedEntries = createDelayedEntryList(1000);
        queue.addFirst(delayedEntries);

        assertEquals(1000, queue.size());
    }

    @Test(expected = ReachedMaxSizeException.class)
    public void testWBQMaxSizeException() {
        final WriteBehindQueue<DelayedEntry> queue = createBoundedWBQ();
        // put total 1001 items. Max allowed is 1000.
        addEnd(1001, queue);
    }

    @Test(expected = ReachedMaxSizeException.class)
    public void testWBQMaxSizeException_withMultipleWBQ() {
        final AtomicInteger counter = new AtomicInteger(0);
        final WriteBehindQueue<DelayedEntry> queue1 = createBoundedWBQ(counter);
        final WriteBehindQueue<DelayedEntry> queue2 = createBoundedWBQ(counter);
        final WriteBehindQueue<DelayedEntry> queue3 = createBoundedWBQ(counter);
        final WriteBehindQueue<DelayedEntry> queue4 = createBoundedWBQ(counter);
        // put total 1001 items. Max allowed is 1000.
        addEnd(10, queue1);
        addEnd(500, queue2);
        addEnd(400, queue3);
        addEnd(91, queue4);
    }

    @Test
    public void testWBQ_counter_is_zero() {
        final AtomicInteger counter = new AtomicInteger(0);
        final WriteBehindQueue<DelayedEntry> queue = createBoundedWBQ(counter);
        addEnd(1000, queue);
        queue.clear();

        assertEquals(0, counter.intValue());
    }


    @Test
    public void testOffer_thenRemove_thenOffer() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();
        addEnd(1000, queue);

        queue.clear();

        addEnd(1000, queue);

        assertEquals(1000, queue.size());
    }

    @Test
    public void testCounter_offer_thenRemove() {
        final AtomicInteger counter = new AtomicInteger(0);
        final WriteBehindQueue<DelayedEntry> queue = createBoundedWBQ(counter);
        addEnd(1000, queue);
        queue.drainTo(new ArrayList<DelayedEntry>(1000));

        assertEquals(0, counter.intValue());
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

        addEnd(1000, queue);

        queue.clear();

        assertEquals(0, queue.size());
    }


    @Test
    public void testRemoveAll() {
        final WriteBehindQueue<DelayedEntry> queue = createWBQ();

        addEnd(1000, queue);

        queue.clear();

        assertEquals(0, queue.size());
    }


    @Test
    public void testGet_onCoalescedWBQ_whenCount_smallerThanQueueSize() throws Exception {
        int queueSize = 100;
        int fetchNumberOfEntries = 10;
        WriteBehindQueue wbq = createWBQ();

        testGetWithCount(wbq, queueSize, fetchNumberOfEntries);
    }

    @Test
    public void testGet_onBoundedWBQ_whenCount_smallerThanQueueSize() throws Exception {
        int queueSize = 100;
        int fetchNumberOfEntries = 10;
        WriteBehindQueue wbq = createBoundedWBQ();

        testGetWithCount(wbq, queueSize, fetchNumberOfEntries);
    }

    @Test
    public void testGet_onCoalescedWBQ_whenCount_higherThanQueueSize() throws Exception {
        int queueSize = 100;
        int fetchNumberOfEntries = 10000;
        WriteBehindQueue wbq = createWBQ();

        testGetWithCount(wbq, queueSize, fetchNumberOfEntries);
    }

    @Test
    public void testGet_onBoundedWBQ_whenCount_higherThanQueueSize() throws Exception {
        int queueSize = 100;
        int fetchNumberOfEntries = 10000;
        WriteBehindQueue wbq = createBoundedWBQ();

        testGetWithCount(wbq, queueSize, fetchNumberOfEntries);
    }

    private void testGetWithCount(WriteBehindQueue<DelayedEntry> queue, int queueSize, int fetchNumberOfEntries) {
        final List<DelayedEntry> delayedEntries = createDelayedEntryList(queueSize);
        for (DelayedEntry entry : delayedEntries) {
            queue.addLast(entry);
        }
        List<DelayedEntry> entries = new ArrayList<DelayedEntry>();
        queue.getFrontByNumber(fetchNumberOfEntries, entries);

        int expectedFetchedEntryCount = Math.min(queueSize, fetchNumberOfEntries);
        assertEquals(expectedFetchedEntryCount, entries.size());
    }

    private void addEnd(int numberOfEntriesToAdd, WriteBehindQueue<DelayedEntry> queue) {
        List<DelayedEntry> delayedEntries = createDelayedEntryList(numberOfEntriesToAdd);
        for (DelayedEntry entry : delayedEntries) {
            queue.addLast(entry);
        }
    }

    private List<DelayedEntry> createDelayedEntryList(int numberOfEntriesToCreate) {
        final List<DelayedEntry> list = new ArrayList<DelayedEntry>(numberOfEntriesToCreate);
        SerializationService ss1 = new DefaultSerializationServiceBuilder().build();
        final long storeTime = Clock.currentTimeMillis();
        for (int i = 0; i < numberOfEntriesToCreate; i++) {
            final DelayedEntry<Data, Object> e = createWithNullValue(ss1.toData(i), storeTime, i);
            list.add(e);
        }
        return list;
    }

    private WriteBehindQueue createBoundedWBQ() {
        final AtomicInteger counter = new AtomicInteger(0);
        return createBoundedWBQ(counter);
    }

    private WriteBehindQueue createBoundedWBQ(AtomicInteger counter) {
        final int maxSizePerNode = 1000;
        return createBoundedWriteBehindQueue(maxSizePerNode, counter);
    }

    private WriteBehindQueue createWBQ() {
        return createDefaultWriteBehindQueue();
    }

}
