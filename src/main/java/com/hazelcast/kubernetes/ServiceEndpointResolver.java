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

import com.hazelcast.kubernetes.KubernetesClient.Endpoints;
import com.hazelcast.kubernetes.KubernetesClient.EntrypointAddress;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.util.StringUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class ServiceEndpointResolver
        extends HazelcastKubernetesDiscoveryStrategy.EndpointResolver {

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
        return new RetryKubernetesClient(new DefaultKubernetesClient(kubernetesMaster, token));
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
        Map<String, Object> properties = entrypointAddress.getAdditionalProperties();
        String ip = entrypointAddress.getIp();
        InetAddress inetAddress = mapAddress(ip);
        int port = (this.port > 0) ? this.port : getServicePort(properties);
        Address address = new Address(inetAddress, port);
        discoveredNodes.add(new SimpleDiscoveryNode(address, properties));
        if (logger.isFinestEnabled()) {
            logger.finest("Found node service with address: " + address);
        }
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
