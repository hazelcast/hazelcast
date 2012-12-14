/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;

import java.net.InetSocketAddress;

public class TestUtility {

    public static HazelcastClient newHazelcastClient(String groupName, String groupPassword, String address) {
        ClientConfig config = new ClientConfig();
        config.getGroupConfig().setName(groupName);
        config.getGroupConfig().setPassword(groupPassword);
        config.addAddress(address);
        config.setUpdateAutomatic(true);
        return HazelcastClient.newHazelcastClient(config);
    }

    public synchronized static HazelcastClient newHazelcastClient(HazelcastInstance... h) {
        String name = h[0].getConfig().getGroupConfig().getName();
        String pass = h[0].getConfig().getGroupConfig().getPassword();
        return newHazelcastClient(ClientProperties.createBaseClientProperties(name, pass), h);
    }

    public synchronized static HazelcastClient newHazelcastClient(ClientProperties properties, String... address) {
        ClientConfig clientConfig = toClientConfig(properties);
        clientConfig.addAddress(address);
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    public synchronized static HazelcastClient newHazelcastClient(ClientProperties properties, HazelcastInstance... h) {
        ClientConfig clientConfig = toClientConfig(properties);
        for (int i = 0; i < h.length; i++) {
            InetSocketAddress inetSocketAddress = h[i].getCluster().getLocalMember().getInetSocketAddress();
            clientConfig.addInetSocketAddress(inetSocketAddress);
        }
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    public static ClientConfig toClientConfig(ClientProperties properties) {
        String groupName = properties.getProperty(ClientProperties.ClientPropertyName.GROUP_NAME);
        String groupPassword = properties.getProperty(ClientProperties.ClientPropertyName.GROUP_PASSWORD);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(groupName, groupPassword));
        clientConfig.setConnectionTimeout(properties.getInteger(ClientProperties.ClientPropertyName.CONNECTION_TIMEOUT));
        clientConfig.setInitialConnectionAttemptLimit(properties.getInteger(ClientProperties.ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT));
        clientConfig.setReconnectionAttemptLimit(properties.getInteger(ClientProperties.ClientPropertyName.RECONNECTION_ATTEMPTS_LIMIT));
        clientConfig.setReConnectionTimeOut(properties.getInteger(ClientProperties.ClientPropertyName.RECONNECTION_TIMEOUT));
        return clientConfig;
    }

    public synchronized static HazelcastClient getAutoUpdatingClient(HazelcastInstance h1) {
        String address = h1.getCluster().getLocalMember().getInetSocketAddress().toString().substring(1);
        GroupConfig gc = h1.getConfig().getGroupConfig();
        return TestUtility.newHazelcastClient(gc.getName(), gc.getPassword(), address);
    }
}
