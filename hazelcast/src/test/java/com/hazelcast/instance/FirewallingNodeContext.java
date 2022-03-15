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

import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.FirewallingServer;

import java.util.Collections;

/**
 * Creates a {@link DefaultNodeContext} with wrapping its ConnectionManager
 * with a {@link FirewallingServer.FirewallingServerConnectionManager}.
 */
public class FirewallingNodeContext extends DefaultNodeContext {

    @Override
    public Server createServer(Node node, ServerSocketRegistry registry, LocalAddressRegistry addressRegistry) {
        Server server = super.createServer(node, registry, addressRegistry);
        return new FirewallingServer(server, Collections.emptySet());
    }

}
