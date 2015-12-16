package com.hazelcast.instance;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.OutOfMemoryHandlerHelper.tryCloseConnections;
import static com.hazelcast.instance.OutOfMemoryHandlerHelper.tryShutdown;
import static com.hazelcast.instance.OutOfMemoryHandlerHelper.tryStopThreads;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OutOfMemoryHandlerHelperTest extends AbstractOutOfMemoryHandlerTest {

    @Before
    public void setUp() throws Exception {
        initHazelcastInstances();
    }

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(OutOfMemoryHandlerHelper.class);
    }

    @Test
    public void testTryCloseConnections() {
        tryCloseConnections(hazelcastInstance);
    }

    @Test
    public void testTryCloseConnections_shouldDoNothingWithNullInstance() {
        tryCloseConnections(null);
    }

    @Test
    public void testTryCloseConnections_shouldDoNothingWhenThrowableIsThrown() {
        tryCloseConnections(hazelcastInstanceThrowsException);
    }

    @Test
    public void testTryShutdown() {
        tryShutdown(hazelcastInstance);
    }

    @Test
    public void testTryShutdown_shouldDoNothingWithNullInstance() {
        tryShutdown(null);
    }

    @Test
    public void testTryShutdown_shouldDoNothingWhenThrowableIsThrown() {
        tryShutdown(hazelcastInstanceThrowsException);
    }

    @Test
    public void testTryStopThreads() {
        tryStopThreads(hazelcastInstance);
    }

    @Test
    public void testTryStopThreads_shouldDoNothingWithNullInstance() {
        tryStopThreads(null);
    }
}
