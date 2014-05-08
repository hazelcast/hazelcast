package com.hazelcast.map.mapstore;

import com.hazelcast.map.writebehind.DelayedEntry;
import com.hazelcast.map.writebehind.WriteBehindQueue;
import com.hazelcast.map.writebehind.WriteBehindQueues;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindQueueTest extends HazelcastTestSupport {

    @Test
    public void smoke() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.writeBehindQueue(true);

        assertEquals(0, queue.size());
    }

    @Test
    public void testOffer() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.writeBehindQueue(true);

        fillQueue(queue);

        assertEquals(3, queue.size());
    }

    @Test
    public void testRemoveEmpty() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.writeBehindQueue(true);

        queue.removeFirst();

        assertEquals(0, queue.size());
    }

    @Test
    public void testClear() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.writeBehindQueue(true);

        queue.clear();

        assertEquals(0, queue.size());
    }

    @Test
    public void testContains() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.writeBehindQueue(true);

        fillQueue(queue);

        assertEquals(false, queue.contains(DelayedEntry.createEmpty()));
    }

    @Test
    public void testClearFull() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.writeBehindQueue(true);

        fillQueue(queue);

        queue.clear();

        assertEquals(0, queue.size());
    }


    @Test
    public void testRemoveAll() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.writeBehindQueue(true);

        final DelayedEntry<Object, Object> e1 = DelayedEntry.createEmpty();
        final DelayedEntry<Object, Object> e2 = DelayedEntry.createEmpty();
        final DelayedEntry<Object, Object> e3 = DelayedEntry.createEmpty();

        queue.offer(e1);
        queue.offer(e2);
        queue.offer(e3);

        queue.removeFirst();
        queue.removeFirst();
        queue.removeFirst();

        assertEquals(0, queue.size());
    }

    private void fillQueue(WriteBehindQueue queue){
        final DelayedEntry<Object, Object> e1 = DelayedEntry.createEmpty();
        final DelayedEntry<Object, Object> e2 = DelayedEntry.createEmpty();
        final DelayedEntry<Object, Object> e3 = DelayedEntry.createEmpty();

        queue.offer(e1);
        queue.offer(e2);
        queue.offer(e3);
    }

}
