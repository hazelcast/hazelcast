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

package com.hazelcast.kubernetes;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.kubernetes.KubernetesClient.Endpoints;
import com.hazelcast.kubernetes.KubernetesClient.EntrypointAddress;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

class KubernetesApiEndpointResolver
        extends HazelcastKubernetesDiscoveryStrategy.EndpointResolver {

    private final String serviceName;
    private final String serviceLabel;
    private final String serviceLabelValue;
    private final String namespace;
    private final Boolean resolveNotReadyAddresses;
    private final int port;
    private final KubernetesClient client;

    KubernetesApiEndpointResolver(ILogger logger, String serviceName, int port, String serviceLabel, String serviceLabelValue,
                                  String namespace, Boolean resolveNotReadyAddresses, KubernetesClient client) {

        super(logger);

        this.serviceName = serviceName;
        this.port = port;
        this.namespace = namespace;
        this.serviceLabel = serviceLabel;
        this.serviceLabelValue = serviceLabelValue;
        this.resolveNotReadyAddresses = resolveNotReadyAddresses;
        this.client = client;
    }

    @Override
    List<DiscoveryNode> resolve() {
        if (serviceName != null && !serviceName.isEmpty()) {
            return getSimpleDiscoveryNodes(client.endpointsByName(namespace, serviceName));
        } else if (serviceLabel != null && !serviceLabel.isEmpty()) {
            return getSimpleDiscoveryNodes(client.endpointsByLabel(namespace, serviceLabel, serviceLabelValue));
        }
        return getSimpleDiscoveryNodes(client.endpoints(namespace));
    }

    private List<DiscoveryNode> getSimpleDiscoveryNodes(Endpoints endpoints) {
        List<DiscoveryNode> discoveredNodes = new ArrayList<DiscoveryNode>();
        resolveNotReadyAddresses(discoveredNodes, endpoints.getNotReadyAddresses());
        resolveAddresses(discoveredNodes, endpoints.getAddresses());
        return discoveredNodes;
    }

    private void resolveNotReadyAddresses(List<DiscoveryNode> discoveredNodes, List<EntrypointAddress> notReadyAddresses) {
        if (Boolean.TRUE.equals(resolveNotReadyAddresses)) {
            resolveAddresses(discoveredNodes, notReadyAddresses);
        }
    }

    private void resolveAddresses(List<DiscoveryNode> discoveredNodes, List<EntrypointAddress> addresses) {
        for (EntrypointAddress address : addresses) {
            addAddress(discoveredNodes, address);
        }
    }

    private void addAddress(List<DiscoveryNode> discoveredNodes, EntrypointAddress entrypointAddress) {
        String ip = entrypointAddress.getIp();
        InetAddress inetAddress = mapAddress(ip);
        int port = port(entrypointAddress);
        Address address = new Address(inetAddress, port);
        discoveredNodes.add(new SimpleDiscoveryNode(address, entrypointAddress.getAdditionalProperties()));
        if (logger.isFinestEnabled()) {
            logger.finest("Found node service with address: " + address);
        }
    }

    private int port(EntrypointAddress entrypointAddress) {
        if (this.port > 0) {
            return this.port;
        }
        if (entrypointAddress.getPort() != null) {
            return entrypointAddress.getPort();
        }
        return NetworkConfig.DEFAULT_PORT;
    }
}
