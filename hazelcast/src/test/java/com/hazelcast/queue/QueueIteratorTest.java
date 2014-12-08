package com.hazelcast.queue;

import com.hazelcast.core.IQueue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueueIteratorTest extends AbstractQueueTest {

    @Test
    public void testIterator() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        Iterator<String> iterator = queue.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            Object o = iterator.next();
            assertEquals(o, "item" + i++);
        }
    }

    @Test
    public void testIterator_whenQueueEmpty() {
        IQueue<String> queue = newQueue();
        Iterator<String> iterator = queue.iterator();

        assertFalse(iterator.hasNext());
        try {
            assertNull(iterator.next());
            fail();
        } catch (NoSuchElementException e) {
        }
    }

    @Test
    public void testIteratorRemove() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        Iterator<String> iterator = queue.iterator();
        iterator.next();
        try {
            iterator.remove();
            fail();
        } catch (UnsupportedOperationException e) {
        }

        assertEquals(10, queue.size());
    }
}
