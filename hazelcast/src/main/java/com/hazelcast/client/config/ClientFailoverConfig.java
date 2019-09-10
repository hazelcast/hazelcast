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

package com.hazelcast.client.config;

import java.util.LinkedList;
import java.util.List;

/**
 * Config class to configure multiple client configs to be used by single client instance
 * The client will try to connect them in given order.
 * When the connected cluster fails or the client blacklisted from the cluster via the management center, the client will
 * search for alternative clusters with given configs.
 */
public class ClientFailoverConfig {

    private int tryCount = Integer.MAX_VALUE;
    private List<ClientConfig> clientConfigs = new LinkedList<>();

    public ClientFailoverConfig() {

    }

    public ClientFailoverConfig addClientConfig(ClientConfig clientConfig) {
        clientConfigs.add(clientConfig);
        return this;
    }

    public ClientFailoverConfig setTryCount(int tryCount) {
        this.tryCount = tryCount;
        return this;
    }

    public List<ClientConfig> getClientConfigs() {
        return clientConfigs;
    }

    public ClientFailoverConfig setClientConfigs(List<ClientConfig> clientConfigs) {
        this.clientConfigs = clientConfigs;
        return this;
    }

    public int getTryCount() {
        return tryCount;
    }

    @Override
    public String toString() {
        return "ClientFailoverConfig{"
                + "tryCount=" + tryCount
                + ", clientConfigs=" + clientConfigs
                + '}';
    }
}
