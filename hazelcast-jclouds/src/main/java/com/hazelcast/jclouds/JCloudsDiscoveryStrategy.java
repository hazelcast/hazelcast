/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jclouds;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import org.jclouds.compute.domain.NodeMetadata;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JClouds implementation of {@link DiscoveryStrategy}
 */
public class JCloudsDiscoveryStrategy implements DiscoveryStrategy {

    private static final ILogger LOGGER = Logger.getLogger(JCloudsDiscoveryStrategy.class);
    private final ComputeServiceBuilder computeServiceBuilder;
    /**
     * Instantiates a new JCloudsDiscoveryStrategy
     *
     * @param properties the properties
     */
    public JCloudsDiscoveryStrategy(Map<String, Comparable> properties) {
        this.computeServiceBuilder = new ComputeServiceBuilder(properties);
    }

    protected JCloudsDiscoveryStrategy(ComputeServiceBuilder computeServiceBuilder) {
        this.computeServiceBuilder = computeServiceBuilder;
    }

    @Override
    public void start() {
        this.computeServiceBuilder.build();
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        List<DiscoveryNode> discoveryNodes = new ArrayList<DiscoveryNode>();
        try {
            Iterable<? extends NodeMetadata> nodes =  computeServiceBuilder.getFilteredNodes();
            for (NodeMetadata metadata : nodes) {
                if (metadata.getStatus() != NodeMetadata.Status.RUNNING) {
                    continue;
                }
                discoveryNodes.add(buildDiscoveredNode(metadata));
            }
            if (discoveryNodes.isEmpty()) {
                LOGGER.warning("No running nodes discovered in configured cloud provider.");
            } else {
                StringBuilder sb = new StringBuilder("Discovered the following nodes with public IPS:\n");
                for (DiscoveryNode node : discoveryNodes) {
                    sb.append("    ").append(node.getPublicAddress().toString()).append("\n");
                }
                LOGGER.finest(sb.toString());
            }
        } catch (Exception e) {
            throw new HazelcastException("Failed to get registered addresses", e);
        }
        return discoveryNodes;
    }

    @Override
    public void destroy() {
        computeServiceBuilder.destroy();
    }

    private DiscoveryNode buildDiscoveredNode(NodeMetadata metadata) {
        Address privateAddressInstance = null;
        if (!metadata.getPrivateAddresses().isEmpty()) {
            InetAddress privateAddress = mapAddress(metadata.getPrivateAddresses().iterator().next());
            privateAddressInstance =  new Address(privateAddress, computeServiceBuilder.getServicePort());
        }

        Address publicAddressInstance = null;
        if (!metadata.getPublicAddresses().isEmpty()) {
            InetAddress publicAddress = mapAddress(metadata.getPublicAddresses().iterator().next());
            publicAddressInstance =  new Address(publicAddress, computeServiceBuilder.getServicePort());
        }

        return new SimpleDiscoveryNode(privateAddressInstance, publicAddressInstance);
    }

    private InetAddress mapAddress(String address) {
        if (address == null) {
            return null;
        }
        try {
            return InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            throw new InvalidConfigurationException("Address '" + address + "' could not be resolved");
        }
    }
}
