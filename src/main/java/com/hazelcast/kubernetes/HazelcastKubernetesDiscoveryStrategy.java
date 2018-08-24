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
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.util.StringUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_API_TOKEN;
import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_MASTER_URL;
import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_SYSTEM_PREFIX;
import static com.hazelcast.kubernetes.KubernetesProperties.NAMESPACE;
import static com.hazelcast.kubernetes.KubernetesProperties.RESOLVE_NOT_READY_ADDRESSES;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_DNS;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_DNS_TIMEOUT;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_LABEL_NAME;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_LABEL_VALUE;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_NAME;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_PORT;

final class HazelcastKubernetesDiscoveryStrategy
        extends AbstractDiscoveryStrategy {

    private static final String DEFAULT_MASTER_URL = "https://kubernetes.default.svc";
    private static final String HAZELCAST_SERVICE_PORT = "hazelcast-service-port";
    private static final int DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS = 5;

    private final EndpointResolver endpointResolver;

    HazelcastKubernetesDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);

        String serviceDns = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_DNS);
        int serviceDnsTimeout
                = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_DNS_TIMEOUT, DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS);
        String serviceName = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_NAME);
        int port = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_PORT, 0);
        String serviceLabel = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_LABEL_NAME);
        String serviceLabelValue = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_LABEL_VALUE, "true");
        String namespace = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, NAMESPACE, getNamespaceOrDefault());
        Boolean resolveNotReadyAddresses = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, RESOLVE_NOT_READY_ADDRESSES, false);
        String kubernetesMaster = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, KUBERNETES_MASTER_URL, DEFAULT_MASTER_URL);
        String apiToken = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, KUBERNETES_API_TOKEN, null);

        logger.info("Kubernetes Discovery properties: { "
                + "service-dns: " + serviceDns + ", "
                + "service-dns-timeout: " + serviceDnsTimeout + ", "
                + "service-name: " + serviceName + ", "
                + "service-port: " + port + ", "
                + "service-label: " + serviceLabel + ", "
                + "service-label-value: " + serviceLabelValue + ", "
                + "namespace: " + namespace + ", "
                + "resolve-not-ready-addresses: " + resolveNotReadyAddresses + ", "
                + "kubernetes-master: " + kubernetesMaster + "}");

        EndpointResolver endpointResolver;
        if (serviceDns != null) {
            endpointResolver = new DnsEndpointResolver(logger, serviceDns, port, serviceDnsTimeout);
        } else {
            endpointResolver = new ServiceEndpointResolver(logger, serviceName, port, serviceLabel, serviceLabelValue,
                    namespace, resolveNotReadyAddresses, kubernetesMaster, apiToken);
        }
        logger.info("Kubernetes Discovery activated resolver: " + endpointResolver.getClass().getSimpleName());
        this.endpointResolver = endpointResolver;
    }

    private String getNamespaceOrDefault() {
        String namespace = System.getenv("KUBERNETES_NAMESPACE");
        if (namespace == null) {
            namespace = System.getenv("OPENSHIFT_BUILD_NAMESPACE");
            if (namespace == null) {
                namespace = "default";
            }
        }
        return namespace;
    }

    public void start() {
        endpointResolver.start();
    }

    public Iterable<DiscoveryNode> discoverNodes() {
        return endpointResolver.resolve();
    }

    public void destroy() {
        endpointResolver.destroy();
    }

    protected <T extends Comparable> T getOrNull(Map<String, Comparable> properties, String prefix, PropertyDefinition property) {
        return getOrDefault(properties, prefix, property, null);
    }

    protected <T extends Comparable> T getOrDefault(Map<String, Comparable> properties, String prefix,
                                                    PropertyDefinition property, T defaultValue) {
        if (property == null) {
            return defaultValue;
        }

        Comparable value = readProperty(prefix, property);
        if (value == null) {
            value = properties.get(property.key());
        }

        if (value == null) {
            return defaultValue;
        }

        return (T) value;
    }

    private Comparable readProperty(String prefix, PropertyDefinition property) {
        if (prefix != null) {
            String p = getProperty(prefix, property);
            String v = System.getProperty(p);
            if (StringUtil.isNullOrEmpty(v)) {
                v = System.getenv(p);
                if (StringUtil.isNullOrEmpty(v)) {
                    v = System.getenv(cIdentifierLike(p));
                }
            }

            if (!StringUtil.isNullOrEmpty(v)) {
                return property.typeConverter().convert(v);
            }
        }
        return null;
    }

    private String cIdentifierLike(String property) {
        property = property.toUpperCase();
        property = property.replace(".", "_");
        return property.replace("-", "_");
    }

    private String getProperty(String prefix, PropertyDefinition property) {
        StringBuilder sb = new StringBuilder(prefix);
        if (prefix.charAt(prefix.length() - 1) != '.') {
            sb.append('.');
        }
        return sb.append(property.key()).toString();
    }

    abstract static class EndpointResolver {
        protected final ILogger logger;

        EndpointResolver(ILogger logger) {
            this.logger = logger;
        }

        abstract List<DiscoveryNode> resolve();

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

        protected int getServicePort(Map<String, Object> properties) {
            int port = NetworkConfig.DEFAULT_PORT;
            if (properties != null) {
                String servicePort = (String) properties.get(HAZELCAST_SERVICE_PORT);
                if (servicePort != null) {
                    port = Integer.parseInt(servicePort);
                }
            }
            return port;
        }
    }
}
