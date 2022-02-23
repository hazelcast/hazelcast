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

package com.hazelcast.instance;

import com.hazelcast.auditlog.impl.NoOpAuditlogService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cluster.Address;
import com.hazelcast.cp.internal.persistence.NopCPPersistenceService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.internal.dynamicconfig.ClusterWideConfigurationService;
import com.hazelcast.internal.dynamicconfig.DynamicConfigListener;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.internal.memory.DefaultMemoryStats;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.version.Version;
import com.hazelcast.wan.impl.WanReplicationService;
import org.mockito.ArgumentMatchers;

import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNodeContext implements NodeContext {

    private final Address address;
    private final NodeExtension nodeExtension = mock(NodeExtension.class);
    private final Server server;

    public TestNodeContext() throws UnknownHostException {
        this(mockServer());
    }

    public TestNodeContext(Server server) throws UnknownHostException {
        this(new Address("127.0.0.1", 5000), server);
    }

    public TestNodeContext(Address address, Server server) {
        this.address = address;
        this.server = server;
    }

    public NodeExtension getNodeExtension() {
        return nodeExtension;
    }

    @Override
    public NodeExtension createNodeExtension(Node node) {
        when(nodeExtension.createService(MapService.class)).thenReturn(mock(MapService.class));
        when(nodeExtension.createService(ICacheService.class)).thenReturn(mock(ICacheService.class));
        when(nodeExtension.createService(WanReplicationService.class)).thenReturn(mock(WanReplicationService.class));
        when(nodeExtension.createService(eq(ClusterWideConfigurationService.class), any()))
                .thenAnswer(
                        invocation -> new ClusterWideConfigurationService(
                                (NodeEngine) invocation.getArguments()[1],
                                mock(DynamicConfigListener.class)
                        )
                );
        when(nodeExtension.createSerializationService()).thenReturn(new DefaultSerializationServiceBuilder().build());
        when(nodeExtension.isStartCompleted()).thenReturn(true);
        when(nodeExtension.isNodeVersionCompatibleWith(any(Version.class))).thenReturn(true);
        when(nodeExtension.getMemoryStats()).thenReturn(new DefaultMemoryStats());
        when(nodeExtension.createMemberUuid()).thenReturn(UuidUtil.newUnsecureUUID());
        when(nodeExtension.getCPPersistenceService()).thenReturn(new NopCPPersistenceService());
        when(nodeExtension.getAuditlogService()).thenReturn(NoOpAuditlogService.INSTANCE);
        return nodeExtension;
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        return new TestAddressPicker(address);
    }

    @Override
    public Joiner createJoiner(Node node) {
        return null;
    }

    @Override
    public Server createServer(Node node, ServerSocketRegistry registry, LocalAddressRegistry addressRegistry) {
        return server;
    }

    static class TestAddressPicker implements AddressPicker {

        final Address address;

        TestAddressPicker(Address address) {
            this.address = address;
        }

        @Override
        public void pickAddress() {
        }

        @Override
        public Address getBindAddress(EndpointQualifier qualifier) {
            return address;
        }

        @Override
        public Address getPublicAddress(EndpointQualifier qualifier) {
            return address;
        }

        @Override
        public Map<EndpointQualifier, Address> getPublicAddressMap() {
            return Collections.singletonMap(EndpointQualifier.MEMBER, address);
        }

        @Override
        public Map<EndpointQualifier, Address> getBindAddressMap() {
            return Collections.singletonMap(EndpointQualifier.MEMBER, address);
        }

        @Override
        public ServerSocketChannel getServerSocketChannel(EndpointQualifier qualifier) {
            return null;
        }

        @Override
        public Map<EndpointQualifier, ServerSocketChannel> getServerSocketChannels() {
            return null;
        }
    }

    private static Server mockServer() {
        Server server = mock(Server.class);
        when(server.getConnectionManager(ArgumentMatchers.<EndpointQualifier>any())).thenReturn(mock(ServerConnectionManager.class));
        return server;
    }
}
