package com.hazelcast.tpc.engine;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.tpc.engine.EventloopQueue;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReactorQueueTest {

    @Test
    public void test(){
        EventloopQueue reactorQueue = new EventloopQueue(16);

        assertNull(reactorQueue.poll());

        assertFalse(reactorQueue.addAndCheckBlocked("1"));
        assertEquals("1", reactorQueue.poll());
        assertTrue(reactorQueue.commitAndMarkBlocked());

        assertTrue(reactorQueue.addAndCheckBlocked("2"));
        assertFalse(reactorQueue.addAndCheckBlocked("3"));
        assertEquals("2", reactorQueue.poll());
        assertEquals("3", reactorQueue.poll());
        assertFalse(reactorQueue.addAndCheckBlocked("4"));
        assertFalse(reactorQueue.commitAndMarkBlocked());
        assertEquals("4",reactorQueue.poll());
        assertTrue(reactorQueue.commitAndMarkBlocked());
    }
}
