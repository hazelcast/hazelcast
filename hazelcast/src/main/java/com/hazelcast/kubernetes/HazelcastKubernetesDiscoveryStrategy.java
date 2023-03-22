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

package com.hazelcast.kubernetes;

import com.hazelcast.instance.impl.ClusterTopologyIntentTracker;
import com.hazelcast.kubernetes.KubernetesConfig.DiscoveryMode;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class HazelcastKubernetesDiscoveryStrategy
        extends AbstractDiscoveryStrategy {
    private final EndpointResolver endpointResolver;

    private final Map<String, String> memberMetadata = new HashMap<>();

    HazelcastKubernetesDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties,
                                         ClusterTopologyIntentTracker clusterTopologyIntentTracker) {
        super(logger, properties);

        KubernetesConfig config = new KubernetesConfig(properties);
        logger.info(config.toString());

        if (DiscoveryMode.DNS_LOOKUP.equals(config.getMode())) {
            endpointResolver = new DnsEndpointResolver(logger, config);
        } else {
            endpointResolver = new KubernetesApiEndpointResolver(logger, config, clusterTopologyIntentTracker);
        }

        logger.info("Kubernetes Discovery activated with mode: " + config.getMode().name());
    }

    @Override
    public void start() {
        endpointResolver.start();
    }

    @Override
    public Map<String, String> discoverLocalMetadata() {
        if (memberMetadata.isEmpty()) {
            memberMetadata.put(PartitionGroupMetaData.PARTITION_GROUP_ZONE, endpointResolver.resolveCurrentZone());
            memberMetadata.put("hazelcast.partition.group.node", endpointResolver.resolveCurrentNodeName());
        }
        return memberMetadata;
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        return endpointResolver.resolveNodes();
    }

    @Override
    public void destroy() {
        endpointResolver.destroy();
    }

    abstract static class EndpointResolver {
        protected final ILogger logger;

        EndpointResolver(ILogger logger) {
            this.logger = logger;
        }

        abstract List<DiscoveryNode> resolveNodes();

        /**
         * Discovers the availability zone in which the current Hazelcast member is running.
         * <p>
         * Note: ZONE_AWARE is available only for the Kubernetes API Mode.
         */
        String resolveCurrentZone() {
            return "unknown";
        }

        /**
         * Discovers the name of the node which the current Hazelcast member pod is running on.
         * <p>
         * Note: NODE_AWARE is available only for the Kubernetes API Mode.
         */
        String resolveCurrentNodeName() {
            return "unknown";
        }

        void start() {
        }

        void destroy() {
        }

        protected InetAddress mapAddress(String address) {
            if (address == null) {
                return null;
            }
            try {
                return InetAddress.getByName(address);
            } catch (UnknownHostException e) {
                logger.warning("Address '" + address + "' could not be resolved");
            }
            return null;
        }
    }
}
