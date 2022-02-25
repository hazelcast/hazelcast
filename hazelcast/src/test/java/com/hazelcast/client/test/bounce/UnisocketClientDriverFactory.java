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

package com.hazelcast.client.test.bounce;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Address;

/**
 * Creates unisocket clients configured to connect to the cluster's steady member.
 */
public class UnisocketClientDriverFactory extends AbstractClientDriverFactory {

    public UnisocketClientDriverFactory() {
    }

    public UnisocketClientDriverFactory(ClientConfig clientConfig) {
        super(clientConfig);
    }

    /**
     * Client config for a unisocket client that connects to the given steady member only. If this instance was
     * constructed with a specific {@link ClientConfig}, then its network config is processed to ensure the client
     * connects to the steady member, otherwise a new {@link ClientConfig} is created.
     */
    @Override
    protected ClientConfig getClientConfig(HazelcastInstance member) {
        // get client configuration that guarantees our client will connect to the specific member
        ClientConfig config = clientConfig == null ? new ClientConfig() : clientConfig;
        config.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        config.getNetworkConfig().setSmartRouting(false);

        Address address = member.getCluster().getLocalMember().getAddress();

        config.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());

        return config;
    }
}
