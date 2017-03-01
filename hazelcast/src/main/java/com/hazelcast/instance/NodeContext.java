/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Joiner;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.spi.annotation.PrivateApi;

import java.nio.channels.ServerSocketChannel;

/**
 * A context for node to provide its dependencies. Acts as a dependency factory.
 * <p/>
 * Normally, there is a default context. But to be able to make tests simpler,
 * to run them faster and in-parallel, it's necessary to avoid network and some heavy-weight
 * objects creations. That's why most of the tests use a special purpose NodeContext.
 */
@PrivateApi
public interface NodeContext {

    NodeExtension createNodeExtension(Node node);

    AddressPicker createAddressPicker(Node node);

    Joiner createJoiner(Node node);

    ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel);
}
