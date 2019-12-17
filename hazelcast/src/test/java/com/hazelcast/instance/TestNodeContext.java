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

package com.hazelcast.instance;

import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cp.internal.persistence.NopCPPersistenceService;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.dynamicconfig.DynamicConfigListener;
import com.hazelcast.internal.networking.ServerSocketRegistry;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.internal.memory.DefaultMemoryStats;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.NetworkingService;
import com.hazelcast.version.Version;
import com.hazelcast.wan.impl.WanReplicationService;
import org.mockito.ArgumentMatchers;

import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNodeContext implements NodeContext {

    private final Address address;
    private final NodeExtension nodeExtension = mock(NodeExtension.class);
    private final NetworkingService networkingService;

    public TestNodeContext() throws UnknownHostException {
        this(mockNs());
    }

    public TestNodeContext(NetworkingService networkingService) throws UnknownHostException {
        this(new Address("127.0.0.1", 5000), networkingService);
    }

    public TestNodeContext(Address address, NetworkingService networkingService) {
        this.address = address;
        this.networkingService = networkingService;
    }

    public NodeExtension getNodeExtension() {
        return nodeExtension;
    }

    @Override
    public NodeExtension createNodeExtension(Node node) {
        when(nodeExtension.createService(MapService.class)).thenReturn(mock(MapService.class));
        when(nodeExtension.createService(ICacheService.class)).thenReturn(mock(ICacheService.class));
        when(nodeExtension.createService(WanReplicationService.class)).thenReturn(mock(WanReplicationService.class));
        when(nodeExtension.createSerializationService()).thenReturn(new DefaultSerializationServiceBuilder().build());
        when(nodeExtension.isStartCompleted()).thenReturn(true);
        when(nodeExtension.isNodeVersionCompatibleWith(any(Version.class))).thenReturn(true);
        when(nodeExtension.getMemoryStats()).thenReturn(new DefaultMemoryStats());
        when(nodeExtension.createMemberUuid()).thenReturn(UuidUtil.newUnsecureUUID());
        when(nodeExtension.createDynamicConfigListener()).thenReturn(mock(DynamicConfigListener.class));
        when(nodeExtension.getCPPersistenceService()).thenReturn(new NopCPPersistenceService());
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
    public NetworkingService createNetworkingService(Node node, ServerSocketRegistry registry) {
        return networkingService;
    }

    static class TestAddressPicker implements AddressPicker {

        final Address address;

        TestAddressPicker(Address address) {
            this.address = address;
        }

        @Override
        public void pickAddress() throws Exception {
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
        public ServerSocketChannel getServerSocketChannel(EndpointQualifier qualifier) {
            return null;
        }

        @Override
        public Map<EndpointQualifier, ServerSocketChannel> getServerSocketChannels() {
            return null;
        }
    }

    private static NetworkingService mockNs() {
        NetworkingService ns = mock(NetworkingService.class);
        when(ns.getEndpointManager(ArgumentMatchers.<EndpointQualifier>any())).thenReturn(mock(EndpointManager.class));
        return ns;
    }
}
