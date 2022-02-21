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

package com.hazelcast.kubernetes;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.kubernetes.KubernetesClient.Endpoint;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
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
    private final String podLabel;
    private final String podLabelValue;
    private final Boolean resolveNotReadyAddresses;
    private final int port;
    private final KubernetesClient client;

    KubernetesApiEndpointResolver(ILogger logger, String serviceName, int port,
                                  String serviceLabel, String serviceLabelValue, String podLabel, String podLabelValue,
                                  Boolean resolveNotReadyAddresses, KubernetesClient client) {

        super(logger);

        this.serviceName = serviceName;
        this.port = port;
        this.serviceLabel = serviceLabel;
        this.serviceLabelValue = serviceLabelValue;
        this.podLabel = podLabel;
        this.podLabelValue = podLabelValue;
        this.resolveNotReadyAddresses = resolveNotReadyAddresses;
        this.client = client;
    }

    @Override
    List<DiscoveryNode> resolve() {
        if (serviceName != null && !serviceName.isEmpty()) {
            logger.fine("Using service name to discover nodes.");
            return getSimpleDiscoveryNodes(client.endpointsByName(serviceName));
        } else if (serviceLabel != null && !serviceLabel.isEmpty()) {
            logger.fine("Using service label to discover nodes.");
            return getSimpleDiscoveryNodes(client.endpointsByServiceLabel(serviceLabel, serviceLabelValue));
        } else if (podLabel != null && !podLabel.isEmpty()) {
            logger.fine("Using pod label to discover nodes.");
            return getSimpleDiscoveryNodes(client.endpointsByPodLabel(podLabel, podLabelValue));
        }
        return getSimpleDiscoveryNodes(client.endpoints());
    }

    private List<DiscoveryNode> getSimpleDiscoveryNodes(List<Endpoint> endpoints) {
        List<DiscoveryNode> discoveredNodes = new ArrayList<DiscoveryNode>();
        for (Endpoint address : endpoints) {
            addAddress(discoveredNodes, address);
        }
        return discoveredNodes;
    }

    private void addAddress(List<DiscoveryNode> discoveredNodes, Endpoint endpoint) {
        if (Boolean.TRUE.equals(resolveNotReadyAddresses) || endpoint.isReady()) {
            Address privateAddress = createAddress(endpoint.getPrivateAddress());
            Address publicAddress = createAddress(endpoint.getPublicAddress());
            discoveredNodes
                    .add(new SimpleDiscoveryNode(privateAddress, publicAddress, endpoint.getAdditionalProperties()));
            if (logger.isFinestEnabled()) {
                logger.finest(String.format("Found node service with addresses (private, public): %s, %s ", privateAddress,
                        publicAddress));
            }
        }
    }

    private Address createAddress(KubernetesClient.EndpointAddress address) {
        if (address == null) {
            return null;
        }
        String ip = address.getIp();
        InetAddress inetAddress = mapAddress(ip);
        int port = port(address);
        return new Address(inetAddress, port);
    }

    private int port(KubernetesClient.EndpointAddress address) {
        if (this.port > 0) {
            return this.port;
        }
        if (address.getPort() != null) {
            return address.getPort();
        }
        return NetworkConfig.DEFAULT_PORT;
    }
}
