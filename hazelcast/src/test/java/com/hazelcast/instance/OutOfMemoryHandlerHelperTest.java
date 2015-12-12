package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.instance.OutOfMemoryHandlerHelper.tryCloseConnections;
import static com.hazelcast.instance.OutOfMemoryHandlerHelper.tryShutdown;
import static com.hazelcast.instance.OutOfMemoryHandlerHelper.tryStopThreads;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class OutOfMemoryHandlerHelperTest extends HazelcastTestSupport {

    private HazelcastInstanceImpl hazelcastInstance;
    private HazelcastInstanceImpl hazelcastInstanceThrowsException;

    @Before
    public void setUp() throws Exception {
        Config config = new Config();

        ConnectionManager connectionManager = mock(ConnectionManager.class);
        doThrow(new OutOfMemoryError()).when(connectionManager).shutdown();

        NodeContext nodeContext = new TestNodeContext();
        NodeContext nodeContextWithThrowable = new TestNodeContext(connectionManager);

        hazelcastInstance = new HazelcastInstanceImpl("OutOfMemoryHandlerHelper", config, nodeContext);
        hazelcastInstanceThrowsException = new HazelcastInstanceImpl("OutOfMemoryHandlerHelperThrowsException", config,
                nodeContextWithThrowable);
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
