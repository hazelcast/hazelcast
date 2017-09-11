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

package com.hazelcast.client.quorum;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;

import static com.hazelcast.test.HazelcastTestSupport.getNode;

public class QuorumTestUtil {

    private QuorumTestUtil() {
    }

    public static ClientConfig getClientConfig(HazelcastInstance instance) {
        ClientConfig clientConfig = new ClientConfig();
        Address address = getNode(instance).address;
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
        clientConfig.getNetworkConfig().setSmartRouting(false);
        clientConfig.getGroupConfig().setName(instance.getConfig().getGroupConfig().getName());
        return clientConfig;
    }
}
