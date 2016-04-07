package com.hazelcast.internal.util.collection;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.concurrent.NoOpIdleStrategy;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MPSCQueueTest extends HazelcastTestSupport {

    @Test(expected = NullPointerException.class)
    public void offer_whenNull() {
        MPSCQueue queue = new MPSCQueue(null, new NoOpIdleStrategy());
        queue.offer(null);
    }

    @Test
    public void offer_whenNotNull() {
        MPSCQueue queue = new MPSCQueue(null, new NoOpIdleStrategy());
        assertTrue(queue.offer(1));
        assertTrue(queue.offer(2));

        assertEquals(2, queue.size());
    }

    @Test
    public void poll_whenEmpty() {
        MPSCQueue queue = new MPSCQueue(null, new NoOpIdleStrategy());
        assertNull(queue.poll());
    }

    @Test
    public void poll_whenNotEmpty() {
        MPSCQueue queue = new MPSCQueue(null, new NoOpIdleStrategy());
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);

        assertEquals(1, queue.poll());
        assertEquals(0, queue.size());

        assertEquals(2, queue.poll());
        assertEquals(0, queue.size());

        assertEquals(3, queue.poll());
        assertEquals(0, queue.size());
    }

    @Test
    public void take_whenNotEmpty() throws Exception {
        MPSCQueue queue = new MPSCQueue(Thread.currentThread(), new NoOpIdleStrategy());
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);

        assertEquals(1, queue.take());
        assertEquals(0, queue.size());

        assertEquals(2, queue.take());
        assertEquals(0, queue.size());

        assertEquals(3, queue.take());
        assertEquals(0, queue.size());
    }
}
