/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.util.StringUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

class ServiceEndpointResolver extends HazelcastKubernetesDiscoveryStrategy.EndpointResolver {

    private final String serviceName;
    private final String serviceLabel;
    private final String serviceLabelValue;
    private final String namespace;
    private final Boolean resolveNotReadyAddresses;
    private final int port;
    private final KubernetesClient client;

    ServiceEndpointResolver(ILogger logger, String serviceName, int port, String serviceLabel, String serviceLabelValue,
                            String namespace, Boolean resolveNotReadyAddresses, String kubernetesMaster, String apiToken) {

        super(logger);

        this.serviceName = serviceName;
        this.port = port;
        this.namespace = namespace;
        this.serviceLabel = serviceLabel;
        this.serviceLabelValue = serviceLabelValue;
        this.resolveNotReadyAddresses = resolveNotReadyAddresses;
        this.client = buildKubernetesClient(apiToken, kubernetesMaster);
    }

    private KubernetesClient buildKubernetesClient(String token, String kubernetesMaster) {
        if (StringUtil.isNullOrEmpty(token)) {
            token = getAccountToken();
        }
        logger.info("Kubernetes Discovery: Bearer Token { " + token + " }");
        Config config = new ConfigBuilder().withOauthToken(token).withMasterUrl(kubernetesMaster).build();
        return new DefaultKubernetesClient(config);
    }

    @Override
    List<DiscoveryNode> resolve() {
        if (serviceName != null && !serviceName.isEmpty()) {
            return getSimpleDiscoveryNodes(client.endpoints().inNamespace(namespace).withName(serviceName).get());
        } else if (serviceLabel != null && !serviceLabel.isEmpty()) {
            return getDiscoveryNodes(client.endpoints().inNamespace(namespace).withLabel(serviceLabel, serviceLabelValue).list());
        }
        return getNodesByNamespace();
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
        List<EndpointSubset> endpointsSubsets = endpoints.getSubsets();
        if (endpointsSubsets == null) {
            return Collections.emptyList();
        }
        List<DiscoveryNode> discoveredNodes = new ArrayList<DiscoveryNode>();
        for (EndpointSubset endpointSubset : endpointsSubsets) {
            resolveNotReadyAddresses(discoveredNodes, endpointSubset);
            resolveAddresses(discoveredNodes, endpointSubset);
        }
        return discoveredNodes;
    }

    private void resolveNotReadyAddresses(List<DiscoveryNode> discoveredNodes, EndpointSubset endpointSubset) {
        if (Boolean.TRUE.equals(resolveNotReadyAddresses)) {
            for (EndpointAddress endpointAddress : endpointSubset.getNotReadyAddresses()) {
                addAddress(discoveredNodes, endpointAddress);
            }
        }
    }

    private void resolveAddresses(List<DiscoveryNode> discoveredNodes, EndpointSubset endpointSubset) {
        List<EndpointAddress> endpointAddresses = endpointSubset.getAddresses();
        if (endpointAddresses == null) {
            return;
        }
        for (EndpointAddress endpointAddress : endpointAddresses) {
            addAddress(discoveredNodes, endpointAddress);
        }
    }

    private void addAddress(List<DiscoveryNode> discoveredNodes, EndpointAddress endpointAddress) {
        Map<String, Object> properties = endpointAddress.getAdditionalProperties();
        String ip = endpointAddress.getIp();
        InetAddress inetAddress = mapAddress(ip);
        int port = (this.port > 0) ? this.port : getServicePort(properties);
        logger.fine("Discovered node: " + ip + " port: " + port);
        Address address = new Address(inetAddress, port);
        discoveredNodes.add(new SimpleDiscoveryNode(address, properties));
    }

    @Override
    void destroy() {
        client.close();
    }

    @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
    private String getAccountToken() {
        return readFileContents("/var/run/secrets/kubernetes.io/serviceaccount/token");
    }

    protected static String readFileContents(String tokenFile) {
        InputStream is = null;
        try {
            File file = new File(tokenFile);
            byte[] data = new byte[(int) file.length()];
            is = new FileInputStream(file);
            is.read(data);
            return new String(data, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException("Could not get token file", e);
        } finally {
            IOUtil.closeResource(is);
        }
    }
}
