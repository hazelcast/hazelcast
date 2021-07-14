/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.StringUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_API_RETIRES;
import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_API_TOKEN;
import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_CA_CERTIFICATE;
import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_MASTER_URL;
import static com.hazelcast.kubernetes.KubernetesProperties.KUBERNETES_SYSTEM_PREFIX;
import static com.hazelcast.kubernetes.KubernetesProperties.NAMESPACE;
import static com.hazelcast.kubernetes.KubernetesProperties.POD_LABEL_NAME;
import static com.hazelcast.kubernetes.KubernetesProperties.POD_LABEL_VALUE;
import static com.hazelcast.kubernetes.KubernetesProperties.RESOLVE_NOT_READY_ADDRESSES;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_DNS;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_DNS_TIMEOUT;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_LABEL_NAME;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_LABEL_VALUE;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_NAME;
import static com.hazelcast.kubernetes.KubernetesProperties.SERVICE_PORT;
import static com.hazelcast.kubernetes.KubernetesProperties.USE_NODE_NAME_AS_EXTERNAL_ADDRESS;

/**
 * Responsible for fetching, parsing, and validating Hazelcast Kubernetes Discovery Strategy input properties.
 */
@SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
final class KubernetesConfig {
    private static final String DEFAULT_MASTER_URL = "https://kubernetes.default.svc";
    private static final int DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS = 5;
    private static final int DEFAULT_KUBERNETES_API_RETRIES = 3;

    // Parameters for DNS Lookup mode
    private final String serviceDns;
    private final int serviceDnsTimeout;

    // Parameters for Kubernetes API mode
    private final String serviceName;
    private final String serviceLabelName;
    private final String serviceLabelValue;
    private final String namespace;
    private final String podLabelName;
    private final String podLabelValue;
    private final boolean resolveNotReadyAddresses;
    private final boolean useNodeNameAsExternalAddress;
    private final int kubernetesApiRetries;
    private final String kubernetesMasterUrl;
    private final String kubernetesApiToken;
    private final String kubernetesCaCertificate;

    // Parameters for both DNS Lookup and Kubernetes API modes
    private final int servicePort;

    KubernetesConfig(Map<String, Comparable> properties) {
        this.serviceDns = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_DNS);
        this.serviceDnsTimeout
                = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_DNS_TIMEOUT, DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS);
        this.serviceName = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_NAME);
        this.serviceLabelName = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_LABEL_NAME);
        this.serviceLabelValue = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_LABEL_VALUE, "true");
        this.podLabelName = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, POD_LABEL_NAME);
        this.podLabelValue = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, POD_LABEL_VALUE);
        this.resolveNotReadyAddresses = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, RESOLVE_NOT_READY_ADDRESSES, true);
        this.useNodeNameAsExternalAddress
                = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, USE_NODE_NAME_AS_EXTERNAL_ADDRESS, false);
        this.kubernetesApiRetries
                = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, KUBERNETES_API_RETIRES, DEFAULT_KUBERNETES_API_RETRIES);
        this.kubernetesMasterUrl = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, KUBERNETES_MASTER_URL, DEFAULT_MASTER_URL);
        this.kubernetesApiToken = getApiToken(properties);
        this.kubernetesCaCertificate = caCertificate(properties);
        this.servicePort = getOrDefault(properties, KUBERNETES_SYSTEM_PREFIX, SERVICE_PORT, 0);
        this.namespace = getNamespaceWithFallbacks(properties, KUBERNETES_SYSTEM_PREFIX, NAMESPACE);

        validateConfig();
    }

    private String getNamespaceWithFallbacks(Map<String, Comparable> properties,
                                             String kubernetesSystemPrefix,
                                             PropertyDefinition propertyDefinition) {
        String namespace = getOrNull(properties, kubernetesSystemPrefix, propertyDefinition);

        if (namespace == null) {
            namespace = System.getenv("KUBERNETES_NAMESPACE");
        }

        if (namespace == null) {
            namespace = System.getenv("OPENSHIFT_BUILD_NAMESPACE");
        }

        if (namespace == null && getMode() == DiscoveryMode.KUBERNETES_API) {
            namespace = readNamespace();
        }

        return namespace;
    }

    private String getApiToken(Map<String, Comparable> properties) {
        String apiToken = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, KUBERNETES_API_TOKEN);
        if (apiToken == null && getMode() == DiscoveryMode.KUBERNETES_API) {
            apiToken = readAccountToken();
        }
        return apiToken;
    }

    private String caCertificate(Map<String, Comparable> properties) {
        String caCertificate = getOrNull(properties, KUBERNETES_SYSTEM_PREFIX, KUBERNETES_CA_CERTIFICATE);
        if (caCertificate == null && getMode() == DiscoveryMode.KUBERNETES_API) {
            caCertificate = readCaCertificate();
        }
        return caCertificate;
    }

    @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
    private static String readAccountToken() {
        return readFileContents("/var/run/secrets/kubernetes.io/serviceaccount/token");
    }

    @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
    private static String readCaCertificate() {
        return readFileContents("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt");
    }

    @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
    private static String readNamespace() {
        return readFileContents("/var/run/secrets/kubernetes.io/serviceaccount/namespace");
    }

    static String readFileContents(String fileName) {
        InputStream is = null;
        try {
            File file = new File(fileName);
            byte[] data = new byte[(int) file.length()];
            is = new FileInputStream(file);
            is.read(data);
            return new String(data, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Could not get " + fileName, e);
        } finally {
            IOUtil.closeResource(is);
        }
    }

    private <T extends Comparable> T getOrNull(Map<String, Comparable> properties, String prefix, PropertyDefinition property) {
        return getOrDefault(properties, prefix, property, null);
    }

    private <T extends Comparable> T getOrDefault(Map<String, Comparable> properties, String prefix,
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

    private void validateConfig() {
        if (!StringUtil.isNullOrEmptyAfterTrim(serviceDns) && (!StringUtil.isNullOrEmptyAfterTrim(serviceName)
                || !StringUtil.isNullOrEmptyAfterTrim(serviceLabelName) || !StringUtil.isNullOrEmptyAfterTrim(podLabelName))) {
            throw new InvalidConfigurationException(
                    String.format("Properties '%s' and ('%s' or '%s' or %s) cannot be defined at the same time",
                            SERVICE_DNS.key(), SERVICE_NAME.key(), SERVICE_LABEL_NAME.key(), POD_LABEL_NAME.key()));
        }
        if (!StringUtil.isNullOrEmptyAfterTrim(serviceName) && !StringUtil.isNullOrEmptyAfterTrim(serviceLabelName)) {
            throw new InvalidConfigurationException(
                    String.format("Properties '%s' and '%s' cannot be defined at the same time",
                            SERVICE_NAME.key(), SERVICE_LABEL_NAME.key()));
        }
        if (!StringUtil.isNullOrEmptyAfterTrim(serviceName) && !StringUtil.isNullOrEmptyAfterTrim(podLabelName)) {
            throw new InvalidConfigurationException(
                    String.format("Properties '%s' and '%s' cannot be defined at the same time",
                            SERVICE_NAME.key(), POD_LABEL_NAME.key()));
        }
        if (!StringUtil.isNullOrEmptyAfterTrim(serviceLabelName) && !StringUtil.isNullOrEmptyAfterTrim(podLabelName)) {
            throw new InvalidConfigurationException(
                    String.format("Properties '%s' and '%s' cannot be defined at the same time",
                            SERVICE_LABEL_NAME.key(), POD_LABEL_NAME.key()));
        }
        if (serviceDnsTimeout < 0) {
            throw new InvalidConfigurationException(
                    String.format("Property '%s' cannot be a negative number", SERVICE_DNS_TIMEOUT.key()));
        }
        if (kubernetesApiRetries < 0) {
            throw new InvalidConfigurationException(
                    String.format("Property '%s' cannot be a negative number", KUBERNETES_API_RETIRES.key()));
        }
        if (servicePort < 0) {
            throw new InvalidConfigurationException(
                    String.format("Property '%s' cannot be a negative number", SERVICE_PORT.key()));
        }
    }

    DiscoveryMode getMode() {
        if (!StringUtil.isNullOrEmptyAfterTrim(serviceDns)) {
            return DiscoveryMode.DNS_LOOKUP;
        } else {
            return DiscoveryMode.KUBERNETES_API;
        }
    }

    String getServiceDns() {
        return serviceDns;
    }

    int getServiceDnsTimeout() {
        return serviceDnsTimeout;
    }

    String getServiceName() {
        return serviceName;
    }

    String getServiceLabelName() {
        return serviceLabelName;
    }

    String getServiceLabelValue() {
        return serviceLabelValue;
    }

    String getNamespace() {
        return namespace;
    }

    public String getPodLabelName() {
        return podLabelName;
    }

    public String getPodLabelValue() {
        return podLabelValue;
    }

    boolean isResolveNotReadyAddresses() {
        return resolveNotReadyAddresses;
    }

    boolean isUseNodeNameAsExternalAddress() {
        return useNodeNameAsExternalAddress;
    }

    int getKubernetesApiRetries() {
        return kubernetesApiRetries;
    }

    String getKubernetesMasterUrl() {
        return kubernetesMasterUrl;
    }

    String getKubernetesApiToken() {
        return kubernetesApiToken;
    }

    String getKubernetesCaCertificate() {
        return kubernetesCaCertificate;
    }

    int getServicePort() {
        return servicePort;
    }

    @Override
    public String toString() {
        return "Kubernetes Discovery properties: { "
                + "service-dns: " + serviceDns + ", "
                + "service-dns-timeout: " + serviceDnsTimeout + ", "
                + "service-name: " + serviceName + ", "
                + "service-port: " + servicePort + ", "
                + "service-label: " + serviceLabelName + ", "
                + "service-label-value: " + serviceLabelValue + ", "
                + "namespace: " + namespace + ", "
                + "pod-label: " + podLabelName + ", "
                + "pod-label-value: " + podLabelValue + ", "
                + "resolve-not-ready-addresses: " + resolveNotReadyAddresses + ", "
                + "use-node-name-as-external-address: " + useNodeNameAsExternalAddress + ", "
                + "kubernetes-api-retries: " + kubernetesApiRetries + ", "
                + "kubernetes-master: " + kubernetesMasterUrl + "}";
    }

    enum DiscoveryMode {
        DNS_LOOKUP,
        KUBERNETES_API
    }
}
