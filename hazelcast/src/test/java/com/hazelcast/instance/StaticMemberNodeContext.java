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

import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.networking.ServerSocketRegistry;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.NetworkingService;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.UUID;

public class StaticMemberNodeContext implements NodeContext {
    private final NodeContext delegate;
    private final UUID uuid;

    public StaticMemberNodeContext(TestHazelcastInstanceFactory factory, Member member) {
        this(factory, member.getUuid(), member.getAddress());
    }

    public StaticMemberNodeContext(TestHazelcastInstanceFactory factory, UUID uuid, Address address) {
        this.uuid = uuid;
        delegate = factory.getRegistry().createNodeContext(address);
    }

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return new DefaultNodeExtension(node) {
            @Override
            public UUID createMemberUuid() {
                return uuid;
            }
        };
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        return delegate.createAddressPicker(node);
    }

    @Override
    public Joiner createJoiner(Node node) {
        return delegate.createJoiner(node);
    }

    @Override
    public NetworkingService createNetworkingService(Node node, ServerSocketRegistry registry) {
        return delegate.createNetworkingService(node, registry);
    }
}
