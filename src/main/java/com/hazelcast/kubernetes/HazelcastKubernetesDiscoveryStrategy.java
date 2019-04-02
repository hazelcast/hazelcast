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

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;
import com.hazelcast.util.StringUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_API_RETIRES;
import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_API_TOKEN;
import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_CA_CERTIFICATE;
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
    private static final int DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS = 5;
    private static final int DEFAULT_KUBERNETES_API_RETRIES = 3;

    private final String namespace;

    private final KubernetesClient client;
    private final EndpointResolver endpointResolver;

    private final Map<String, Object> memberMetadata = new HashMap<String, Object>();

    HazelcastKubernetesDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);

        String serviceDns = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_DNS);
        int serviceDnsTimeout
                = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_DNS_TIMEOUT, DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS);
        String serviceName = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_NAME);
        int port = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_PORT, 0);
        String serviceLabel = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_LABEL_NAME);
        String serviceLabelValue = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_LABEL_VALUE, "true");
        namespace = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, NAMESPACE, getNamespaceOrDefault());
        Boolean resolveNotReadyAddresses = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, RESOLVE_NOT_READY_ADDRESSES, false);
        int kubernetesApiRetries
                = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, KUBERNETES_API_RETIRES, DEFAULT_KUBERNETES_API_RETRIES);
        String kubernetesMaster = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, KUBERNETES_MASTER_URL, DEFAULT_MASTER_URL);
        String apiToken = getApiToken(properties);
        String caCertificate = caCertificate(properties);

        logger.info("Kubernetes Discovery properties: { "
                + "service-dns: " + serviceDns + ", "
                + "service-dns-timeout: " + serviceDnsTimeout + ", "
                + "service-name: " + serviceName + ", "
                + "service-port: " + port + ", "
                + "service-label: " + serviceLabel + ", "
                + "service-label-value: " + serviceLabelValue + ", "
                + "namespace: " + namespace + ", "
                + "resolve-not-ready-addresses: " + resolveNotReadyAddresses + ", "
                + "kubernetes-api-retries: " + kubernetesApiRetries + ", "
                + "kubernetes-master: " + kubernetesMaster + "}");

        client = buildKubernetesClient(namespace, kubernetesMaster, apiToken, caCertificate, kubernetesApiRetries);
        if (serviceDns != null) {
            endpointResolver = new DnsEndpointResolver(logger, serviceDns, port, serviceDnsTimeout);
        } else {
            endpointResolver = new KubernetesApiEndpointResolver(logger, serviceName, port, serviceLabel, serviceLabelValue,
                    resolveNotReadyAddresses, client);
        }
        logger.info("Kubernetes Discovery activated resolver: " + endpointResolver.getClass().getSimpleName());
    }

    private String getApiToken(Map<String, Comparable> properties) {
        String apiToken = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, KUBERNETES_API_TOKEN);
        if (apiToken == null) {
            apiToken = readAccountToken();
        }
        return apiToken;
    }

    private String caCertificate(Map<String, Comparable> properties) {
        String caCertificate = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, KUBERNETES_CA_CERTIFICATE);
        if (caCertificate == null) {
            caCertificate = readCaCertificate();
        }
        return caCertificate;
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

    @Override
    public Map<String, Object> discoverLocalMetadata() {
        if (memberMetadata.isEmpty()) {
            memberMetadata.put(PartitionGroupMetaData.PARTITION_GROUP_ZONE, discoverZone());
        }
        return memberMetadata;
    }

    private String discoverZone() {
        try {
            String podName = System.getenv("POD_NAME");
            if (podName == null) {
                podName = System.getenv("HOSTNAME");
            }
            if (podName == null) {
                podName = InetAddress.getLocalHost().getHostName();
            }
            String zone = client.zone(podName);
            if (zone != null) {
                return zone;
            }
        } catch (Exception e) {
            // only log the exception and the message, Hazelcast should still start
            getLogger().finest(e);
        }
        getLogger().warning("Cannot fetch the current zone, ZONE_AWARE feature is disabled");
        return "unknown";
    }

    @Override
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
    }

    private KubernetesClient buildKubernetesClient(String namespace, String kubernetesMaster, String accessToken,
                                                   String caCertificate, int retries) {
        return new KubernetesClient(namespace, kubernetesMaster, accessToken, caCertificate, retries);
    }

    @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
    private static String readAccountToken() {
        return readFileContents("/var/run/secrets/kubernetes.io/serviceaccount/token");
    }

    @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
    private static String readCaCertificate() {
        return readFileContents("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt");
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
