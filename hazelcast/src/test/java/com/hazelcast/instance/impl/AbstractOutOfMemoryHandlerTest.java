/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.TestNodeContext;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.NetworkStats;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractOutOfMemoryHandlerTest extends HazelcastTestSupport {
    private static final AtomicInteger INSTANCE_ID_COUNTER = new AtomicInteger();

    protected HazelcastInstanceImpl hazelcastInstance;
    protected HazelcastInstanceImpl hazelcastInstanceThrowsException;

    @After
    public void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
        if (hazelcastInstanceThrowsException != null) {
            Server server = hazelcastInstanceThrowsException.node.getServer();
            // Failing connection manager throws error, so we should disable this behaviour to shutdown instance properly
            ((FailingServer) server).switchToDummyMode();
            hazelcastInstanceThrowsException.shutdown();
        }
    }

    protected void initHazelcastInstances() throws Exception {
        Config config = new Config();

        NodeContext nodeContext = new TestNodeContext();
        NodeContext nodeContextWithThrowable = new TestNodeContext(new FailingServer());

        int instanceId = INSTANCE_ID_COUNTER.incrementAndGet();

        String instanceName = "OutOfMemoryHandlerHelper" + instanceId;
        String instanceThrowsExceptionName = "OutOfMemoryHandlerHelperThrowsException" + instanceId;

        hazelcastInstance = new HazelcastInstanceImpl(instanceName, config, nodeContext);
        hazelcastInstanceThrowsException = new HazelcastInstanceImpl(instanceThrowsExceptionName, config,
                nodeContextWithThrowable);
    }

    private static class FailingServer implements Server {

        private boolean dummyMode;

        ServerConnectionManager dummy = new ServerConnectionManager() {
            @Override
            public Server getServer() {
                return null;
            }

            @Override
            public @Nonnull Collection getConnections() {
                return Collections.emptyList();
            }

            @Override
            public ServerConnection get(@Nonnull Address address, int streamId) {
                return null;
            }

            @Override
            @Nonnull
            public List<ServerConnection> getAllConnections(@Nonnull Address address) {
                return Collections.emptyList();
            }

            @Override
            public ServerConnection getOrConnect(@Nonnull Address address, int streamId) {
                return null;
            }

            @Override
            public ServerConnection getOrConnect(@Nonnull Address address, boolean silent, int streamId) {
                return null;
            }

            @Override
            public boolean register(
                    Address remoteAddress,
                    Address targetAddress,
                    Collection<Address> remoteAddressAliases,
                    UUID remoteUuid,
                    ServerConnection connection,
                    int streamId
            ) {
                return false;
            }

            @Override
            public boolean transmit(Packet packet, Address target, int streamId) {
                return false;
            }

            @Override
            public void addConnectionListener(ConnectionListener listener) {
            }

            @Override
            public void accept(Packet o) {
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
        public ServerContext getContext() {
            return null;
        }

        @Override
        public @Nonnull Collection<ServerConnection> getConnections() {
            return Collections.emptyList();
        }

        @Override
        public Map<EndpointQualifier, NetworkStats> getNetworkStats() {
            return null;
        }

        @Override
        public void addConnectionListener(ConnectionListener<ServerConnection> listener) {
        }

        @Override
        public ServerConnectionManager getConnectionManager(EndpointQualifier qualifier) {
            return dummy;
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
