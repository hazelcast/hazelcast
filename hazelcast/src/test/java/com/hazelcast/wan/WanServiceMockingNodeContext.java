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

package com.hazelcast.wan;

import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.internal.server.Server;

import java.util.function.Function;

public class WanServiceMockingNodeContext implements NodeContext {
    private final NodeContext nodeContextDelegate;
    private final Function<Node, NodeExtension> nodeExtensionFn;

    public WanServiceMockingNodeContext(NodeContext nodeContextDelegate,
                                        Function<Node, NodeExtension> nodeExtensionFn) {
        super();
        this.nodeContextDelegate = nodeContextDelegate;
        this.nodeExtensionFn = nodeExtensionFn;
    }

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return nodeExtensionFn.apply(node);
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        return this.nodeContextDelegate.createAddressPicker(node);
    }

    @Override
    public Joiner createJoiner(Node node) {
        return this.nodeContextDelegate.createJoiner(node);
    }

    @Override
    public Server createServer(Node node, ServerSocketRegistry serverSocketRegistry, LocalAddressRegistry addressRegistry) {
        return this.nodeContextDelegate.createServer(node, serverSocketRegistry, addressRegistry);
    }
}
