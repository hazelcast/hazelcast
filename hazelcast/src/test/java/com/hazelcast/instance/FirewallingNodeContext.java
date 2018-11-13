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

import com.hazelcast.internal.networking.ServerSocketRegistry;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.NetworkingService;
import com.hazelcast.nio.tcp.FirewallingNetworkingService;

import java.util.Collections;

/**
 * Creates a {@link DefaultNodeContext} with wrapping its ConnectionManager
 * with a {@link FirewallingConnectionManager}.
 */
public class FirewallingNodeContext extends DefaultNodeContext {

    @Override
    public NetworkingService createNetworkingService(Node node, ServerSocketRegistry registry) {
        NetworkingService networkingService = super.createNetworkingService(node, registry);
        return new FirewallingNetworkingService(networkingService, Collections.<Address>emptySet());
    }

}
