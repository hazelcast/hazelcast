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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.util.StringUtil;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class ServiceEndpointResolver
        extends HazelcastKubernetesDiscoveryStrategy.EndpointResolver {

    private final String serviceName;
    private final String serviceLabel;
    private final String serviceLabelValue;
    private final String namespace;

    private final KubernetesClient client;

    ServiceEndpointResolver(ILogger logger, String serviceName, String serviceLabel, String serviceLabelValue,
                                   String namespace, String kubernetesMaster, String apiToken) {

        super(logger);

        this.serviceName = serviceName;
        this.namespace = namespace;
        this.serviceLabel = serviceLabel;
        this.serviceLabelValue = serviceLabelValue;
        this.client = buildeKubernetesClient(apiToken, kubernetesMaster);
    }

    private KubernetesClient buildeKubernetesClient(String apiToken, String kubernetesMaster) {
        String oauthToken = apiToken;
        if (StringUtil.isNullOrEmpty(oauthToken)) {
            oauthToken = getAccountToken();
        }
        logger.info("Kubernetes Discovery: Bearer Token { " + apiToken + " }");
        Config config = new ConfigBuilder().withOauthToken(oauthToken).withMasterUrl(kubernetesMaster).build();
        return new DefaultKubernetesClient(config);
    }

    List<DiscoveryNode> resolve() {
        List<DiscoveryNode> result = Collections.emptyList();
        if (serviceName != null && !serviceName.isEmpty()) {
            result = getSimpleDiscoveryNodes(client.endpoints().inNamespace(namespace).withName(serviceName).get());
        }

        if (result.isEmpty() && serviceLabel != null && !serviceLabel.isEmpty()) {
            result = getDiscoveryNodes(
                    client.endpoints().inNamespace(namespace).withLabel(serviceLabel, serviceLabelValue).list());
        }

        return result.isEmpty() ? getNodesByNamespace() : result;
    }

    private List<DiscoveryNode> getNodesByNamespace() {
        final EndpointsList endpointsInNamespace = client.endpoints().inNamespace(namespace).list();
        if (endpointsInNamespace == null) {
            return Collections.emptyList();
        }
        return getDiscoveryNodes(endpointsInNamespace);
    }

    private List<DiscoveryNode> getDiscoveryNodes(EndpointsList endpointsInNamespace) {
        if (endpointsInNamespace == null) {
            return Collections.emptyList();
        }
        List<DiscoveryNode> discoveredNodes = new ArrayList<DiscoveryNode>();
        for (Endpoints endpoints : endpointsInNamespace.getItems()) {
            discoveredNodes.addAll(getSimpleDiscoveryNodes(endpoints));
        }
        return discoveredNodes;
    }

    private List<DiscoveryNode> getSimpleDiscoveryNodes(Endpoints endpoints) {
        if (endpoints == null) {
            return Collections.emptyList();
        }
        List<DiscoveryNode> discoveredNodes = new ArrayList<DiscoveryNode>();
        for (EndpointSubset endpointSubset : endpoints.getSubsets()) {
            for (EndpointAddress endpointAddress : endpointSubset.getAddresses()) {
                Map<String, Object> properties = endpointAddress.getAdditionalProperties();
                String ip = endpointAddress.getIp();
                InetAddress inetAddress = mapAddress(ip);
                int port = getServicePort(properties);
                Address address = new Address(inetAddress, port);
                discoveredNodes.add(new SimpleDiscoveryNode(address, properties));
            }
        }
        return discoveredNodes;
    }

    @Override
    void destroy() {
        client.close();
    }

    private String getAccountToken() {
        try {
            String tokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token";
            File file = new File(tokenFile);
            byte[] data = new byte[(int) file.length()];
            InputStream is = new FileInputStream(file);
            is.read(data);
            return new String(data);

        } catch (IOException e) {
            throw new RuntimeException("Could not get token file", e);
        }
    }
}
