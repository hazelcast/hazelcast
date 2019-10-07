/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

import com.hazelcast.config.Config;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.TestNodeContext;
import com.hazelcast.internal.networking.NetworkStats;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.AggregateEndpointManager;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.NetworkingService;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public abstract class AbstractOutOfMemoryHandlerTest extends HazelcastTestSupport {

    protected HazelcastInstanceImpl hazelcastInstance;
    protected HazelcastInstanceImpl hazelcastInstanceThrowsException;

    @After
    public void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
        if (hazelcastInstanceThrowsException != null) {
            NetworkingService networkingService = hazelcastInstanceThrowsException.node.getNetworkingService();
            // Failing connection manager throws error, so we should disable this behaviour to shutdown instance properly
            ((FailingNetworkingService) networkingService).switchToDummyMode();
            hazelcastInstanceThrowsException.shutdown();
        }
    }

    protected void initHazelcastInstances() throws Exception {
        Config config = new Config();

        NodeContext nodeContext = new TestNodeContext();
        NodeContext nodeContextWithThrowable = new TestNodeContext(new FailingNetworkingService());

        hazelcastInstance = new HazelcastInstanceImpl("OutOfMemoryHandlerHelper", config, nodeContext);
        hazelcastInstanceThrowsException = new HazelcastInstanceImpl("OutOfMemoryHandlerHelperThrowsException", config,
                nodeContextWithThrowable);
    }

    private static class FailingNetworkingService
            implements NetworkingService {

        private boolean dummyMode;

        EndpointManager dummy = new EndpointManager() {

            @Override
            public Set getActiveConnections() {
                return null;
            }

            @Override
            public Collection getConnections() {
                return null;
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
            public boolean transmit(Packet packet, Connection connection) {
                return false;
            }

            @Override
            public boolean transmit(Packet packet, Address target) {
                return false;
            }

            @Override
            public void addConnectionListener(ConnectionListener listener) {

            }

            @Override
            public void accept(Object o) {

            }

            @Override
            public NetworkStats getNetworkStats() {
                return null;
            }
        };

        private void switchToDummyMode() {
            dummyMode = true;
        }

        @Override
        public IOService getIoService() {
            return null;
        }

        @Override
        public AggregateEndpointManager getAggregateEndpointManager() {
            return null;
        }

        @Override
        public EndpointManager getEndpointManager(EndpointQualifier qualifier) {
            return dummy;
        }

        @Override
        public void scheduleDeferred(Runnable task, long delay, TimeUnit unit) {

        }

        @Override
        public Networking getNetworking() {
            return null;
        }

        @Override
        public boolean isLive() {
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
