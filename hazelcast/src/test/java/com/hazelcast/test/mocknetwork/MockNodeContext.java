/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Joiner;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.instance.NodeExtensionFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.NodeIOService;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.compatibility.SamplingNodeExtension;

import java.lang.reflect.Constructor;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.Set;

import static com.hazelcast.util.ExceptionUtil.rethrow;

@SuppressWarnings("WeakerAccess")
public class MockNodeContext implements NodeContext {

    private final TestNodeRegistry registry;
    private final Address thisAddress;
    private final Set<Address> initiallyBlockedAddresses;

    protected MockNodeContext(TestNodeRegistry registry, Address thisAddress) {
        this(registry, thisAddress, Collections.<Address>emptySet());
    }

    protected MockNodeContext(TestNodeRegistry registry, Address thisAddress, Set<Address> initiallyBlockedAddresses) {
        this.registry = registry;
        this.thisAddress = thisAddress;
        this.initiallyBlockedAddresses = initiallyBlockedAddresses;
    }

    @Override
    public NodeExtension createNodeExtension(Node node) {
        if (TestEnvironment.isRecordingSerializedClassNames()) {
            return constructSamplingNodeExtension(node);
        } else {
            return NodeExtensionFactory.create(node);
        }
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
    public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
        NodeIOService ioService = new NodeIOService(node, node.nodeEngine);
        ConnectionManager delegate = new MockConnectionManager(ioService, node, registry);
        return new FirewallingConnectionManager(delegate, initiallyBlockedAddresses);
    }

    /**
     * @return {@code NodeExtension} suitable for sampling serialized objects in OSS or EE environment
     */
    @SuppressWarnings("unchecked")
    private NodeExtension constructSamplingNodeExtension(Node node) {
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
            NodeExtension wrapped = NodeExtensionFactory.create(node);
            return new SamplingNodeExtension(wrapped);
        }
    }
}
