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

package com.hazelcast.instance;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;

import java.nio.channels.ServerSocketChannel;
import java.util.Collections;

/**
 * Creates a {@link DefaultNodeContext} with wrapping its ConnectionManager
 * with a {@link FirewallingConnectionManager}.
 */
public class FirewallingNodeContext extends DefaultNodeContext {

    @Override
    public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
        ConnectionManager connectionManager = super.createConnectionManager(node, serverSocketChannel);
        return new FirewallingConnectionManager(connectionManager, Collections.<Address>emptySet());
    }
}
