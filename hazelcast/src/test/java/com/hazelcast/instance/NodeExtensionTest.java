/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Joiner;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanReplicationService;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InOrder;

import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NodeExtensionTest extends HazelcastTestSupport {

    private HazelcastInstanceImpl hazelcastInstance;

    @After
    public void cleanup() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    public void verifyMethods() throws Exception {
        DummyNodeContext nodeContext = new DummyNodeContext(new Address("127.0.0.1", 5000));
        NodeExtension nodeExtension = nodeContext.getNodeExtension();

        hazelcastInstance = new HazelcastInstanceImpl(randomName(), getConfig(), nodeContext);

        InOrder inOrder = inOrder(nodeExtension);

        inOrder.verify(nodeExtension, times(1)).beforeStart();
        inOrder.verify(nodeExtension, times(1)).createSerializationService();
        inOrder.verify(nodeExtension, times(1)).printNodeInfo();
        inOrder.verify(nodeExtension, times(1)).createExtensionServices();
        inOrder.verify(nodeExtension, times(1)).beforeJoin();
        inOrder.verify(nodeExtension, times(1)).afterStart();

        hazelcastInstance.shutdown();
        inOrder.verify(nodeExtension, times(1)).beforeShutdown();
        inOrder.verify(nodeExtension, times(1)).shutdown();
    }

    protected Config getConfig() {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        return config;
    }

    public static class DummyAddressPicker implements AddressPicker {
        final Address address;

        private DummyAddressPicker(Address address) {
            this.address = address;
        }

        @Override
        public void pickAddress() throws Exception {
        }

        @Override
        public Address getBindAddress() {
            return address;
        }

        @Override
        public Address getPublicAddress() {
            return address;
        }

        @Override
        public ServerSocketChannel getServerSocketChannel() {
            return null;
        }
    }

    public static class DummyNodeContext implements NodeContext {

        private final Address address;
        private final NodeExtension nodeExtension = mock(NodeExtension.class);

        public DummyNodeContext() throws UnknownHostException {
            this(new Address("127.0.0.1", 5000));
        }

        public DummyNodeContext(Address address) {
            this.address = address;
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
            return nodeExtension;
        }

        @Override
        public AddressPicker createAddressPicker(Node node) {
            return new DummyAddressPicker(address);
        }

        @Override
        public Joiner createJoiner(Node node) {
            return null;
        }

        @Override
        public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
            return mock(ConnectionManager.class);
        }
    }
}
