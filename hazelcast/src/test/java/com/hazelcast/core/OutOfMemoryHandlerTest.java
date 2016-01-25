package com.hazelcast.core;

import com.hazelcast.instance.AbstractOutOfMemoryHandlerTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OutOfMemoryHandlerTest extends AbstractOutOfMemoryHandlerTest {

    TestOutOfMemoryHandler outOfMemoryHandler;

    @Before
    public void setUp() throws Exception {
        initHazelcastInstances();

        outOfMemoryHandler = new TestOutOfMemoryHandler();
    }

    @Test
    public void testShouldHandle() throws Exception {
        assertTrue(outOfMemoryHandler.shouldHandle(new OutOfMemoryError()));
    }

    @Test
    public void testTryCloseConnections() {
        outOfMemoryHandler.closeConnections(hazelcastInstance);
    }

    @Test
    public void testTryCloseConnections_shouldDoNothingWithNullInstance() {
        outOfMemoryHandler.closeConnections(null);
    }

    @Test
    public void testTryCloseConnections_shouldDoNothingWhenThrowableIsThrown() {
        outOfMemoryHandler.closeConnections(hazelcastInstanceThrowsException);
    }

    @Test
    public void testTryShutdown() {
        outOfMemoryHandler.shutdown(hazelcastInstance);
    }

    @Test
    public void testTryShutdown_shouldDoNothingWithNullInstance() {
        outOfMemoryHandler.shutdown(null);
    }

    @Test
    public void testTryShutdown_shouldDoNothingWhenThrowableIsThrown() {
        outOfMemoryHandler.shutdown(hazelcastInstanceThrowsException);
    }

    @Test
    public void testTryStopThreads() {
        outOfMemoryHandler.stopThreads(hazelcastInstance);
    }

    @Test
    public void testTryStopThreads_shouldDoNothingWithNullInstance() {
        outOfMemoryHandler.stopThreads(null);
    }

    static class TestOutOfMemoryHandler extends OutOfMemoryHandler {

        void closeConnections(HazelcastInstance hazelcastInstance) {
            tryCloseConnections(hazelcastInstance);
        }

        void shutdown(HazelcastInstance hazelcastInstance) {
            tryShutdown(hazelcastInstance);
        }

        void stopThreads(HazelcastInstance hazelcastInstance) {
            tryStopThreads(hazelcastInstance);
        }

        @Override
        public void onOutOfMemory(OutOfMemoryError oome, HazelcastInstance[] hazelcastInstances) {
        }
    }
}
