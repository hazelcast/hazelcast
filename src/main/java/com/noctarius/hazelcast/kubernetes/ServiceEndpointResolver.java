/*
 * Copyright (c) 2015, Christoph Engelbert (aka noctarius) and
 * contributors. All rights reserved.
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
package com.noctarius.hazelcast.kubernetes;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveredNode;
import com.hazelcast.spi.discovery.SimpleDiscoveredNode;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class ServiceEndpointResolver extends HazelcastKubernetesDiscoveryStrategy.EndpointResolver {

    private final String serviceName;
    private final String namespace;

    private final KubernetesClient client;

    public ServiceEndpointResolver(String serviceName, String namespace) {
        this.client = new DefaultKubernetesClient();
        this.serviceName = serviceName;
        this.namespace = namespace;
    }

    List<DiscoveredNode> resolve() {
        Endpoints endpoints = client.endpoints().inNamespace(namespace).withName(serviceName).get();

        List<DiscoveredNode> discoveredNodes = new ArrayList<DiscoveredNode>();
        for (EndpointSubset endpointSubset : endpoints.getSubsets()) {
            for (EndpointAddress endpointAddress : endpointSubset.getAddresses()) {
                Map<String, Object> properties = endpointAddress.getAdditionalProperties();

                String ip = endpointAddress.getIp();
                InetAddress inetAddress = mapAddress(ip);
                int port = getServicePort(properties);

                Address address = new Address(inetAddress, port);
                discoveredNodes.add(new SimpleDiscoveredNode(address, properties));
            }
        }

        return discoveredNodes;
    }

    @Override
    void destroy() {
        client.close();
    }
}
