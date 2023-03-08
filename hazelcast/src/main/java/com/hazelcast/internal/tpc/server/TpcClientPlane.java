/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.server;

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.tpc.TpcSocketConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.TpcEngine;
import com.hazelcast.internal.tpcengine.util.BindRandomPort;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.server.ServerContext.KILO_BYTE;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_KEEPALIVE;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TCP_NODELAY;

/**
 * Contains the logic for TPC aware clients to connect to TPC enabled server.
 */
public class TpcClientPlane {

    private final TpcEngine tpcEngine;
    private final Node node;
    private final Config config;
    private volatile List<Integer> ports = new ArrayList<>();

    public TpcClientPlane(TpcEngine tpcEngine, Node node) {
        this.tpcEngine = tpcEngine;
        this.node = node;
        this.config = node.config;
    }

    public List<Integer> getServerPorts() {
        return ports;
    }

    public void start() {
        TpcSocketConfig clientSocketConfig = getTcpSocketConfig();

        BindRandomPort bindRandomPort = newBindRandomPort(clientSocketConfig);

        for (int k = 0; k < tpcEngine.reactorCount(); k++) {
            Reactor reactor = tpcEngine.reactor(k);
            AsyncServerSocket serverSocket = reactor.newAsyncServerBuilder()
                    .set(SO_RCVBUF, clientSocketConfig.getReceiveBufferSizeKB() * KILO_BYTE)
                    .setAcceptConsumer(acceptRequest -> reactor.newAsyncSocketBuilder(acceptRequest)
                            .setReader(new ClientMessageDecoder(node.clientEngine))
                            .set(SO_SNDBUF, clientSocketConfig.getSendBufferSizeKB() * KILO_BYTE)
                            .set(SO_RCVBUF, clientSocketConfig.getReceiveBufferSizeKB() * KILO_BYTE)
                            .set(TCP_NODELAY, true)
                            .set(SO_KEEPALIVE, true)
                            .build()
                            .start())
                    .build();

            int port = bindRandomPort.bind(serverSocket, null);
            ports.add(port);
            serverSocket.start();
        }
    }

    private static BindRandomPort newBindRandomPort(TpcSocketConfig clientSocketConfig) {
        String[] range = clientSocketConfig.getPortRange().split("-");
        int startPort = Integer.parseInt(range[0]);
        int endPort = Integer.parseInt(range[1]);
        return new BindRandomPort(startPort, endPort);
    }

    // public for testing
    public TpcSocketConfig getTcpSocketConfig() {
        validateSocketConfig();

        if (config.getAdvancedNetworkConfig().isEnabled()) {
            ServerSocketEndpointConfig endpointConfig = (ServerSocketEndpointConfig) config
                    .getAdvancedNetworkConfig()
                    .getEndpointConfigs()
                    .get(EndpointQualifier.CLIENT);

            return endpointConfig.getTpcSocketConfig();
        }

        // unified socket
        return config.getNetworkConfig().getTpcSocketConfig();
    }

    private void validateSocketConfig() {
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        if (!advancedNetworkConfig.isEnabled()) {
            return;
        }

        Map<EndpointQualifier, EndpointConfig> endpointConfigs = advancedNetworkConfig.getEndpointConfigs();

        if (endpointConfigs.get(EndpointQualifier.CLIENT) == null) {
            // Advanced network is enabled yet there is no configured server socket
            // for clients. This means cluster will run but no client ports will be
            // created, so no clients can connect to the cluster.
            throw new InvalidConfigurationException("Missing client server socket configuration. "
                    + "If you have enabled TPC and advanced networking, "
                    + "please configure a client server socket.");
        }
    }
}
