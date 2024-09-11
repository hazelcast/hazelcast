/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.mocknetwork;

import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.instance.impl.NodeExtensionFactory;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.tcp.TcpServerContext;
import com.hazelcast.internal.server.FirewallingServer;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.compatibility.SamplingNodeExtension;

import java.lang.reflect.Constructor;
import java.util.Set;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public class MockNodeContext implements NodeContext {

    private final TestNodeRegistry registry;
    private final Address thisAddress;
    private final Set<Address> initiallyBlockedAddresses;

    MockNodeContext(TestNodeRegistry registry, Address thisAddress, Set<Address> initiallyBlockedAddresses) {
        this.registry = registry;
        this.thisAddress = thisAddress;
        this.initiallyBlockedAddresses = initiallyBlockedAddresses;
    }

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return TestEnvironment.isRecordingSerializedClassNames()
                ? constructSamplingNodeExtension(node)
                : NodeExtensionFactory.create(node, DefaultNodeContext.EXTENSION_PRIORITY_LIST);
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        return new StaticAddressPicker(thisAddress);
    }

    @Override
    public Joiner createJoiner(Node node) {
        return new MockJoiner(node, registry, initiallyBlockedAddresses);
    }

    @Override
    public Server createServer(Node node, ServerSocketRegistry serverSocketRegistry, LocalAddressRegistry addressRegistry) {
        TcpServerContext serverContext = new TcpServerContext(node, node.nodeEngine);
        MockServer mockNetworkingService = new MockServer(serverContext, node, registry);
        return new FirewallingServer(mockNetworkingService, initiallyBlockedAddresses);
    }

    /**
     * @return {@code NodeExtension} suitable for sampling serialized objects in OSS or EE environment
     */
    @SuppressWarnings("unchecked")
    private static NodeExtension constructSamplingNodeExtension(Node node) {
        if (BuildInfoProvider.getBuildInfo().isEnterprise()) {
            try {
                Class<? extends NodeExtension> klass = (Class<? extends NodeExtension>)
                        Class.forName("com.hazelcast.test.compatibility.SamplingEnterpriseNodeExtension");
                Constructor<? extends NodeExtension> constructor = klass.getConstructor(Node.class);
                return constructor.newInstance(node);
            } catch (Exception e) {
                throw rethrow(e);
            }
        } else {
            NodeExtension wrapped = NodeExtensionFactory.create(node, DefaultNodeContext.EXTENSION_PRIORITY_LIST);
            return new SamplingNodeExtension(wrapped);
        }
    }
}
