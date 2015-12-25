package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;

public abstract class AbstractOutOfMemoryHandlerTest extends HazelcastTestSupport {

    protected HazelcastInstanceImpl hazelcastInstance;
    protected HazelcastInstanceImpl hazelcastInstanceThrowsException;

    public void initHazelcastInstances() throws Exception {
        Config config = new Config();

        NodeContext nodeContext = new TestNodeContext();
        NodeContext nodeContextWithThrowable = new TestNodeContext(new FailingConnectionManager());

        hazelcastInstance = new HazelcastInstanceImpl("OutOfMemoryHandlerHelper", config, nodeContext);
        hazelcastInstanceThrowsException = new HazelcastInstanceImpl("OutOfMemoryHandlerHelperThrowsException", config,
                nodeContextWithThrowable);
    }

    @After
    public void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
        if (hazelcastInstanceThrowsException != null) {
            ConnectionManager connectionManager = hazelcastInstanceThrowsException.node.getConnectionManager();
            // Failing connection manager throws error, so we should disable this behaviour to shutdown instance properly
            ((FailingConnectionManager) connectionManager).switchToDummyMode();
            hazelcastInstanceThrowsException.shutdown();
        }
    }

    private static class FailingConnectionManager implements ConnectionManager {

        private boolean dummyMode;

        private void switchToDummyMode() {
            dummyMode = true;
        }

        @Override
        public int getCurrentClientConnections() {
            return 0;
        }

        @Override
        public int getAllTextConnections() {
            return 0;
        }

        @Override
        public int getConnectionCount() {
            return 0;
        }

        @Override
        public int getActiveConnectionCount() {
            return 0;
        }

        @Override
        public Connection getConnection(Address address) {
            return null;
        }

        @Override
        public Connection getOrConnect(Address address) {
            return null;
        }

        @Override
        public Connection getOrConnect(Address address, boolean silent) {
            return null;
        }

        @Override
        public boolean registerConnection(Address address, Connection connection) {
            return false;
        }

        @Override
        public void destroyConnection(Connection connection) {
        }

        @Override
        public void addConnectionListener(ConnectionListener listener) {
        }

        @Override
        public boolean transmit(Packet packet, Connection connection) {
            return false;
        }

        @Override
        public boolean transmit(Packet packet, Address target) {
            return false;
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void shutdown() {
            if (!dummyMode) {
                throw new OutOfMemoryError();
            }
        }

    }

}
