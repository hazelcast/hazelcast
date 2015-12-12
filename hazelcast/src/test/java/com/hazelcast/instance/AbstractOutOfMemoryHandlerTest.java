package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.HazelcastTestSupport;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public abstract class AbstractOutOfMemoryHandlerTest extends HazelcastTestSupport {

    protected HazelcastInstanceImpl hazelcastInstance;
    protected HazelcastInstanceImpl hazelcastInstanceThrowsException;

    public void initHazelcastInstances() throws Exception {
        Config config = new Config();

        ConnectionManager connectionManager = mock(ConnectionManager.class);
        doThrow(new OutOfMemoryError()).when(connectionManager).shutdown();

        NodeContext nodeContext = new TestNodeContext();
        NodeContext nodeContextWithThrowable = new TestNodeContext(connectionManager);

        hazelcastInstance = new HazelcastInstanceImpl("OutOfMemoryHandlerHelper", config, nodeContext);
        hazelcastInstanceThrowsException = new HazelcastInstanceImpl("OutOfMemoryHandlerHelperThrowsException", config,
                nodeContextWithThrowable);
    }
}
