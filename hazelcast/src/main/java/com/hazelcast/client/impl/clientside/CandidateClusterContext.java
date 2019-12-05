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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

/**
 * Carries the information that is specific to one cluster
 */
public class CandidateClusterContext {

    private final String clusterName;
    private final AddressProvider addressProvider;
    private final DiscoveryService discoveryService;
    private final ICredentialsFactory credentialsFactory;
    private final SocketInterceptor socketInterceptor;
    private final ChannelInitializerProvider channelInitializerProvider;

    public CandidateClusterContext(String clusterName, AddressProvider addressProvider, DiscoveryService discoveryService,
                                   ICredentialsFactory credentialsFactory, SocketInterceptor socketInterceptor,
                                   ChannelInitializerProvider channelInitializerProvider) {
        this.clusterName = clusterName;
        this.addressProvider = addressProvider;
        this.discoveryService = discoveryService;
        this.credentialsFactory = credentialsFactory;
        this.socketInterceptor = socketInterceptor;
        this.channelInitializerProvider = channelInitializerProvider;
    }

    public void start() {
        if (discoveryService != null) {
            discoveryService.start();
        }
    }

    public ICredentialsFactory getCredentialsFactory() {
        return credentialsFactory;
    }

    public void destroy() {
        if (discoveryService != null) {
            discoveryService.destroy();
        }
    }

    public AddressProvider getAddressProvider() {
        return addressProvider;
    }

    public SocketInterceptor getSocketInterceptor() {
        return socketInterceptor;
    }

    public String getClusterName() {
        return clusterName;
    }

    public ChannelInitializerProvider getChannelInitializerProvider() {
        return channelInitializerProvider;
    }
}
