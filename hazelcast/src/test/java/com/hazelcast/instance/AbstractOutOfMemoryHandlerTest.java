/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    protected void initHazelcastInstances() throws Exception {
        Config config = new Config();

        NodeContext nodeContext = new TestNodeContext();
        NodeContext nodeContextWithThrowable = new TestNodeContext(new FailingConnectionManager());

        hazelcastInstance = new HazelcastInstanceImpl("OutOfMemoryHandlerHelper", config, nodeContext);
        hazelcastInstanceThrowsException = new HazelcastInstanceImpl("OutOfMemoryHandlerHelperThrowsException", config,
                nodeContextWithThrowable);
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
        public void onConnectionClose(Connection connection) {
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
