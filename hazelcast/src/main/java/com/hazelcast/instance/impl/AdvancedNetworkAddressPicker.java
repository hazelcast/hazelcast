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

package com.hazelcast.instance.impl;

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;

import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * An address picker used to pick addresses for all configured endpoints when the {@link AdvancedNetworkConfig} is used.
 * The picker is composite facade that holds one {@link DefaultAddressPicker} per Endpoint, and delegates each function
 * to the relevant picker.
 */
class AdvancedNetworkAddressPicker
        implements AddressPicker {

    private final AdvancedNetworkConfig advancedNetworkConfig;
    private final Map<EndpointQualifier, AddressPicker> pickers = new HashMap<>();

    AdvancedNetworkAddressPicker(Config config, ILogger logger) {
        this.advancedNetworkConfig = config.getAdvancedNetworkConfig();

        for (EndpointConfig endpointConfig : advancedNetworkConfig.getEndpointConfigs().values()) {
            if (endpointConfig instanceof ServerSocketEndpointConfig) {
                ServerSocketEndpointConfig serverSocketEndpointConfig = (ServerSocketEndpointConfig) endpointConfig;
                EndpointQualifier endpointQualifier = serverSocketEndpointConfig.getQualifier();
                TcpIpConfig tcpIpConfig = advancedNetworkConfig.getJoin().getTcpIpConfig();
                InterfacesConfig interfacesConfig = serverSocketEndpointConfig.getInterfaces();
                String publicAddressConfig = serverSocketEndpointConfig.getPublicAddress();
                boolean isReuseAddress = serverSocketEndpointConfig.isReuseAddress();
                boolean isPortAutoIncrement = serverSocketEndpointConfig.isPortAutoIncrement();
                int port = serverSocketEndpointConfig.getPort();
                int portCount = serverSocketEndpointConfig.getPortCount();

                AddressPicker picker = new DefaultAddressPicker(config, endpointQualifier, interfacesConfig, tcpIpConfig,
                        isReuseAddress, isPortAutoIncrement, port, portCount, publicAddressConfig, logger);
                pickers.put(endpointConfig.getQualifier(), picker);
            }
        }
    }

    @Override
    public void pickAddress()
            throws Exception {
        for (AddressPicker picker : pickers.values()) {
            picker.pickAddress();
        }
    }

    @Override
    public Address getBindAddress(EndpointQualifier qualifier) {
        return pickers.get(qualifier).getBindAddress(qualifier);
    }

    @Override
    public Address getPublicAddress(EndpointQualifier qualifier) {
        return pickers.get(qualifier).getPublicAddress(qualifier);
    }

    @Override
    public Map<EndpointQualifier, Address> getPublicAddressMap() {
        Map<EndpointQualifier, Address> pubAddressMap = new HashMap<>(pickers.size());
        for (Map.Entry<EndpointQualifier, AddressPicker>  entry : pickers.entrySet()) {
            pubAddressMap.put(entry.getKey(), entry.getValue().getPublicAddress(entry.getKey()));
        }

        return pubAddressMap;
    }

    @Override
    public Map<EndpointQualifier, Address> getBindAddressMap() {
        Map<EndpointQualifier, Address> bindAddressMap = new HashMap<>(pickers.size());
        for (Map.Entry<EndpointQualifier, AddressPicker>  entry : pickers.entrySet()) {
            bindAddressMap.put(entry.getKey(), entry.getValue().getBindAddress(entry.getKey()));
        }

        return bindAddressMap;
    }

    @Override
    public ServerSocketChannel getServerSocketChannel(EndpointQualifier qualifier) {
        return pickers.get(qualifier).getServerSocketChannel(qualifier);
    }

    @Override
    public Map<EndpointQualifier, ServerSocketChannel> getServerSocketChannels() {
        Map<EndpointQualifier, ServerSocketChannel> channels = new HashMap<>(pickers.size());
        for (Map.Entry<EndpointQualifier, AddressPicker>  entry : pickers.entrySet()) {
            channels.put(entry.getKey(), entry.getValue().getServerSocketChannel(entry.getKey()));
        }

        return channels;
    }
}
