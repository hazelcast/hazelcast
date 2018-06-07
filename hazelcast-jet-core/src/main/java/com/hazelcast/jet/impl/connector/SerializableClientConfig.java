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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;

import java.io.Serializable;
import java.util.List;

/**
 * Serializable subset of the {@link ClientConfig} which contains address and
 * authentication information to be used to create a Hazelcast Client when the
 * data needs to be fetched from remote cluster.
 */
class SerializableClientConfig implements Serializable {

    private String groupName;
    private String groupPass;
    private List<String> addresses;

    SerializableClientConfig(ClientConfig clientConfig) {
        GroupConfig groupConfig = clientConfig.getGroupConfig();
        List<String> addresses = clientConfig.getNetworkConfig().getAddresses();
        this.groupName = groupConfig.getName();
        this.groupPass = groupConfig.getPassword();
        this.addresses = addresses;
    }

    ClientConfig asClientConfig() {
        ClientConfig config = new ClientConfig();
        config.getGroupConfig().setName(groupName);
        config.getGroupConfig().setPassword(groupPass);
        config.getNetworkConfig().setAddresses(addresses);
        return config;
    }
}
