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

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.kubernetes.KubernetesConfig.ExposeExternallyMode;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.exception.RestClientException;
import com.hazelcast.spi.utils.RestClient;
import com.hazelcast.spi.utils.RetryUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Responsible for connecting to the Kubernetes API.
 *
 * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/">Kubernetes API</a>
 */
@SuppressWarnings("checkstyle:methodcount")
class KubernetesClient {
    private static final ILogger LOGGER = Logger.getLogger(KubernetesClient.class);

    private static final List<String> NON_RETRYABLE_KEYWORDS = asList(
            "\"reason\":\"Forbidden\"",
            "\"reason\":\"NotFound\"",
            "Failure in generating SSLSocketFactory");

    private final String namespace;
    private final String kubernetesMaster;
    private final String caCertificate;
    private final int retries;
    private final KubernetesApiProvider apiProvider;
    private ExposeExternallyMode exposeExternallyMode;
    private final boolean useNodeNameAsExternalAddress;
    private final String servicePerPodLabelName;
    private final String servicePerPodLabelValue;

    private final KubernetesTokenProvider tokenProvider;

    private boolean isNoPublicIpAlreadyLogged;
    private boolean isKnownExceptionAlreadyLogged;

    KubernetesClient(String namespace, String kubernetesMaster, KubernetesTokenProvider tokenProvider,
                     String caCertificate, int retries, ExposeExternallyMode exposeExternallyMode,
                     boolean useNodeNameAsExternalAddress, String servicePerPodLabelName,
                     String servicePerPodLabelValue) {
        this.namespace = namespace;
        this.kubernetesMaster = kubernetesMaster;
        this.tokenProvider = tokenProvider;
        this.caCertificate = caCertificate;
        this.retries = retries;
        this.exposeExternallyMode = exposeExternallyMode;
        this.useNodeNameAsExternalAddress = useNodeNameAsExternalAddress;
        this.servicePerPodLabelName = servicePerPodLabelName;
        this.servicePerPodLabelValue = servicePerPodLabelValue;
        this.apiProvider =  buildKubernetesApiUrlProvider();
    }

    KubernetesClient(String namespace, String kubernetesMaster, KubernetesTokenProvider tokenProvider,
                     String caCertificate, int retries, ExposeExternallyMode exposeExternallyMode,
                     boolean useNodeNameAsExternalAddress, String servicePerPodLabelName,
                     String servicePerPodLabelValue, KubernetesApiProvider apiProvider) {
        this.namespace = namespace;
        this.kubernetesMaster = kubernetesMaster;
        this.tokenProvider = tokenProvider;
        this.caCertificate = caCertificate;
        this.retries = retries;
        this.exposeExternallyMode = exposeExternallyMode;
        this.useNodeNameAsExternalAddress = useNodeNameAsExternalAddress;
        this.servicePerPodLabelName = servicePerPodLabelName;
        this.servicePerPodLabelValue = servicePerPodLabelValue;
        this.apiProvider = apiProvider;
    }

    KubernetesApiProvider buildKubernetesApiUrlProvider() {
        try {
            String endpointSlicesUrlString =
                    String.format("%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices", kubernetesMaster, namespace);
            callGet(endpointSlicesUrlString);
            LOGGER.finest("Using EndpointSlices API to discover endpoints.");
        } catch (Exception e) {
            LOGGER.finest("EndpointSlices are not available, using Endpoints API to discover endpoints.");
            return new KubernetesApiEndpointProvider();
        }
        return new KubernetesApiEndpointSlicesProvider();
    }

    /**
     * Retrieves POD addresses in the specified {@code namespace}.
     *
     * @return all POD addresses
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-143">Kubernetes Endpoint API</a>
     */
    List<Endpoint> endpoints() {
        try {
            String urlString = String.format("%s/api/v1/namespaces/%s/pods", kubernetesMaster, namespace);
            return enrichWithPublicAddresses(parsePodsList(callGet(urlString)));
        } catch (RestClientException e) {
            return handleKnownException(e);
        }
    }

    /**
     * Retrieves POD addresses for all services in the specified {@code namespace} filtered by {@code serviceLabel}
     * and {@code serviceLabelValue}.
     *
     * @param serviceLabel      label used to filter responses
     * @param serviceLabelValue label value used to filter responses
     * @return all POD addresses from the specified {@code namespace} filtered by the label
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-143">Kubernetes Endpoint API</a>
     */
    List<Endpoint> endpointsByServiceLabel(String serviceLabel, String serviceLabelValue) {
        try {
            String param = String.format("labelSelector=%s=%s", serviceLabel, serviceLabelValue);
            String urlString = String.format(apiProvider.getEndpointsByServiceLabelUrlString(),
                    kubernetesMaster, namespace, param);
            return enrichWithPublicAddresses(apiProvider.parseEndpointsList(callGet(urlString)));
        } catch (RestClientException e) {
            return handleKnownException(e);
        }
    }

    /**
     * Retrieves POD addresses from the specified {@code namespace} and the given {@code endpointName}.
     *
     * @param endpointName endpoint name
     * @return all POD addresses from the specified {@code namespace} and the given {@code endpointName}
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-143">Kubernetes Endpoint API</a>
     */
    List<Endpoint> endpointsByName(String endpointName) {
        try {
            String urlString = String.format(apiProvider.getEndpointsByNameUrlString(),
                    kubernetesMaster, namespace, endpointName);
            return enrichWithPublicAddresses(apiProvider.parseEndpoints(callGet(urlString)));
        } catch (RestClientException e) {
            return handleKnownException(e);
        }
    }

    /**
     * Retrieves POD addresses for all services in the specified {@code namespace} filtered by {@code podLabel}
     * and {@code podLabelValue}.
     *
     * @param podLabel      label used to filter responses
     * @param podLabelValue label value used to filter responses
     * @return all POD addresses from the specified {@code namespace} filtered by the label
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-143">Kubernetes Endpoint API</a>
     */
    List<Endpoint> endpointsByPodLabel(String podLabel, String podLabelValue) {
        try {
            String param = String.format("labelSelector=%s=%s", podLabel, podLabelValue);
            String urlString = String.format("%s/api/v1/namespaces/%s/pods?%s", kubernetesMaster, namespace, param);
            return enrichWithPublicAddresses(parsePodsList(callGet(urlString)));
        } catch (RestClientException e) {
            return handleKnownException(e);
        }
    }

    /**
     * Retrieves zone name for the specified {@code namespace} and the given {@code podName}.
     * <p>
     * Note that the Kubernetes environment provides such information as defined
     * <a href="https://kubernetes.io/docs/reference/kubernetes-api/labels-annotations-taints">here</a>.
     *
     * @param podName POD name
     * @return zone name
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11">Kubernetes Endpoint API</a>
     */
    String zone(String podName) {
        String nodeUrlString = String.format("%s/api/v1/nodes/%s", kubernetesMaster, nodeName(podName));
        return extractZone(callGet(nodeUrlString));
    }

    /**
     * Retrieves node name for the specified {@code namespace} and the given {@code podName}.
     *
     * @param podName POD name
     * @return Node name
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11">Kubernetes Endpoint API</a>
     */
    String nodeName(String podName) {
        String podUrlString = String.format("%s/api/v1/namespaces/%s/pods/%s", kubernetesMaster, namespace, podName);
        return extractNodeName(callGet(podUrlString));
    }

    // For test purpose
    boolean isKnownExceptionAlreadyLogged() {
        return isKnownExceptionAlreadyLogged;
    }

    private static List<Endpoint> parsePodsList(JsonObject podsListJson) {
        List<Endpoint> addresses = new ArrayList<>();

        for (JsonValue item : toJsonArray(podsListJson.get("items"))) {
            JsonObject status = item.asObject().get("status").asObject();
            String ip = toString(status.get("podIP"));
            if (ip != null) {
                Integer port = extractContainerPort(item);
                addresses.add(new Endpoint(new EndpointAddress(ip, port), isReady(status)));
            }
        }
        return addresses;
    }

    private static Integer extractContainerPort(JsonValue podItemJson) {
        JsonArray containers = toJsonArray(podItemJson.asObject().get("spec").asObject().get("containers"));
        // If multiple containers are in one POD, then use the default Hazelcast port from the configuration.
        if (containers.size() == 1) {
            JsonValue container = containers.get(0);
            JsonArray ports = toJsonArray(container.asObject().get("ports"));
            // If multiple ports are exposed by a container, then use the default Hazelcast port from the configuration.
            if (ports.size() == 1) {
                JsonValue port = ports.get(0);
                JsonValue containerPort = port.asObject().get("containerPort");
                if (containerPort != null && containerPort.isNumber()) {
                    return containerPort.asInt();
                }
            }
        }
        return null;
    }

    private static boolean isReady(JsonObject podItemStatusJson) {
        for (JsonValue containerStatus : toJsonArray(podItemStatusJson.get("containerStatuses"))) {
            // If multiple containers are in one POD, then each needs to be ready.
            if (!containerStatus.asObject().get("ready").asBoolean()) {
                return false;
            }
        }
        return true;
    }

    private static String extractNodeName(JsonObject podJson) {
        return toString(podJson.get("spec").asObject().get("nodeName"));
    }

    private static String extractZone(JsonObject nodeJson) {
        JsonObject labels = nodeJson.get("metadata").asObject().get("labels").asObject();
        List<String> zoneLabels = asList("topology.kubernetes.io/zone", "failure-domain.kubernetes.io/zone",
                "failure-domain.beta.kubernetes.io/zone");
        for (String zoneLabel : zoneLabels) {
            JsonValue zone = labels.get(zoneLabel);
            if (zone != null) {
                return toString(zone);
            }
        }
        return null;
    }

    /**
     * Tries to add public addresses to the endpoints.
     * <p>
     * If it's not possible, then returns the input parameter.
     * <p>
     * Assigning public IPs must meet one of the following requirements:
     * <ul>
     * <li>Each POD must be exposed with a separate LoadBalancer service OR</li>
     * <li>Each POD must be exposed with a separate NodePort service and Kubernetes nodes must have external IPs</li>
     * </ul>
     * <p>
     * The algorithm to fetch public IPs is as follows:
     * <ol>
     * <li>Use Kubernetes API (/endpoints) to find dedicated services for each POD</li>
     * <li>For each POD:
     * <ol>
     * <li>Use Kubernetes API (/services) to find the LoadBalancer External IP and Service Port</li>
     * <li>If not found, then use Kubernetes API (/nodes) to find External IP of the Node</li>
     * </ol>
     * </li>
     * </ol>
     */
    private List<Endpoint> enrichWithPublicAddresses(List<Endpoint> endpoints) {
        if (exposeExternallyMode == ExposeExternallyMode.DISABLED) {
            return endpoints;
        }
        try {
            String endpointsUrl = String.format(apiProvider.getEndpointsUrlString(), kubernetesMaster, namespace);
            if (!StringUtil.isNullOrEmptyAfterTrim(servicePerPodLabelName)
                    && !StringUtil.isNullOrEmptyAfterTrim(servicePerPodLabelValue)) {
                endpointsUrl += String.format("?labelSelector=%s=%s", servicePerPodLabelName, servicePerPodLabelValue);
            }
            JsonObject endpointsJson = callGet(endpointsUrl);

            List<EndpointAddress> privateAddresses = privateAddresses(endpoints);
            Map<EndpointAddress, String> services = apiProvider.extractServices(endpointsJson, privateAddresses);
            Map<EndpointAddress, String> nodes = apiProvider.extractNodes(endpointsJson, privateAddresses);

            Map<EndpointAddress, String> publicIps = new HashMap<>();
            Map<EndpointAddress, Integer> publicPorts = new HashMap<>();
            Map<String, String> cachedNodePublicIps = new HashMap<>();

            for (Map.Entry<EndpointAddress, String> serviceEntry : services.entrySet()) {
                EndpointAddress privateAddress = serviceEntry.getKey();
                String service = serviceEntry.getValue();
                String serviceUrl = String.format("%s/api/v1/namespaces/%s/services/%s", kubernetesMaster, namespace, service);
                JsonObject serviceJson = callGet(serviceUrl);
                try {
                    String loadBalancerAddress = extractLoadBalancerAddress(serviceJson);
                    Integer servicePort = extractServicePort(serviceJson);
                    publicIps.put(privateAddress, loadBalancerAddress);
                    publicPorts.put(privateAddress, servicePort);
                } catch (Exception e) {
                    // Load Balancer public IP cannot be found, try using NodePort.
                    Integer nodePort = extractNodePort(serviceJson);
                    String node = nodes.get(privateAddress);
                    String nodePublicAddress;
                    if (cachedNodePublicIps.containsKey(node)) {
                        nodePublicAddress = cachedNodePublicIps.get(node);
                    } else {
                        nodePublicAddress = externalAddressForNode(node);
                        cachedNodePublicIps.put(node, nodePublicAddress);
                    }
                    publicIps.put(privateAddress, nodePublicAddress);
                    publicPorts.put(privateAddress, nodePort);
                }
            }

            return createEndpoints(endpoints, publicIps, publicPorts);
        } catch (Exception e) {
            if (exposeExternallyMode == ExposeExternallyMode.ENABLED) {
                throw e;
            }
            // If expose-externally not set (exposeExternallyMode == ExposeExternallyMode.AUTO), silently ignore any exception
            LOGGER.finest(e);
            // Log warning only once.
            if (!isNoPublicIpAlreadyLogged) {
                LOGGER.warning(
                        "Cannot fetch public IPs of Hazelcast Member PODs, you won't be able to use Hazelcast Smart Client from "
                                + "outside of the Kubernetes network");
                isNoPublicIpAlreadyLogged = true;
            }
            return endpoints;
        }
    }

    private static List<EndpointAddress> privateAddresses(List<Endpoint> endpoints) {
        List<EndpointAddress> result = new ArrayList<>();
        for (Endpoint endpoint : endpoints) {
            result.add(endpoint.getPrivateAddress());
        }
        return result;
    }

    private static String extractLoadBalancerAddress(JsonObject serviceResponse) {
        JsonObject ingress = serviceResponse
                .get("status").asObject()
                .get("loadBalancer").asObject()
                .get("ingress").asArray().get(0).asObject();
        JsonValue address = ingress.get("ip");
        if (address == null) {
            address = ingress.get("hostname");
        }
        return address.asString();
    }

    private static Integer extractServicePort(JsonObject serviceJson) {
        JsonArray ports = toJsonArray(serviceJson.get("spec").asObject().get("ports"));
        // Service must have one and only one Node Port assigned.
        if (ports.size() != 1) {
            throw new KubernetesClientException(String.format("Cannot expose externally, service %s needs to have "
                    + "exactly one port defined", serviceJson.get("metadata").asObject().get("name")));
        }
        return ports.get(0).asObject().get("port").asInt();
    }

    private static Integer extractNodePort(JsonObject serviceJson) {
        JsonArray ports = toJsonArray(serviceJson.get("spec").asObject().get("ports"));
        // Service must have one and only one Node Port assigned.
        if (ports.size() != 1) {
            throw new KubernetesClientException(String.format("Cannot expose externally, service %s needs to have "
                    + "exactly one nodePort defined", serviceJson.get("metadata").asObject().get("name")));
        }
        return ports.get(0).asObject().get("nodePort").asInt();
    }

    private String externalAddressForNode(String node) {
        String nodeExternalAddress;
        if (useNodeNameAsExternalAddress) {
            LOGGER.info("Using node name instead of public IP for node, must be available from client: " + node);
            nodeExternalAddress = node;
        } else {
            String nodeUrl = String.format("%s/api/v1/nodes/%s", kubernetesMaster, node);
            nodeExternalAddress = extractNodePublicIp(callGet(nodeUrl));
        }
        return nodeExternalAddress;
    }

    private static String extractNodePublicIp(JsonObject nodeJson) {
        for (JsonValue address : toJsonArray(nodeJson.get("status").asObject().get("addresses"))) {
            if ("ExternalIP".equals(address.asObject().get("type").asString())) {
                return address.asObject().get("address").asString();
            }
        }
        throw new KubernetesClientException(String.format("Cannot expose externally, node %s does not have ExternalIP"
                + " assigned", nodeJson.get("metadata").asObject().get("name")));
    }

    private static List<Endpoint> createEndpoints(List<Endpoint> endpoints, Map<EndpointAddress, String> publicIps,
                                                  Map<EndpointAddress, Integer> publicPorts) {
        List<Endpoint> result = new ArrayList<>();
        for (Endpoint endpoint : endpoints) {
            EndpointAddress privateAddress = endpoint.getPrivateAddress();
            EndpointAddress publicAddress = new EndpointAddress(publicIps.get(privateAddress),
                    publicPorts.get(privateAddress));
            result.add(new Endpoint(privateAddress, publicAddress, endpoint.isReady(), endpoint.getAdditionalProperties()));
        }
        return result;
    }

    /**
     * Makes a REST call to Kubernetes API and returns the result JSON.
     *
     * @param urlString Kubernetes API REST endpoint
     * @return parsed JSON
     * @throws KubernetesClientException if Kubernetes API didn't respond with 200 and a valid JSON content
     */
    private JsonObject callGet(final String urlString) {
        return RetryUtils.retry(() -> Json
                .parse(RestClient.create(urlString)
                        .withHeader("Authorization", String.format("Bearer %s", tokenProvider.getToken()))
                        .withCaCertificates(caCertificate)
                        .get()
                        .getBody())
                .asObject(), retries, NON_RETRYABLE_KEYWORDS);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private List<Endpoint> handleKnownException(RestClientException e) {
        if (e.getHttpErrorCode() == 401) {
            if (!isKnownExceptionAlreadyLogged) {
                LOGGER.warning("Kubernetes API authorization failure! To use Hazelcast Kubernetes discovery, "
                        + "please check your 'api-token' property. Starting standalone.");
                isKnownExceptionAlreadyLogged = true;
            }
        } else if (e.getHttpErrorCode() == 403) {
            if (!isKnownExceptionAlreadyLogged) {
                LOGGER.warning("Kubernetes API access is forbidden! Starting standalone. To use Hazelcast Kubernetes discovery,"
                        + " configure the required RBAC. For 'default' service account in 'default' namespace execute: "
                        + "`kubectl apply -f https://raw.githubusercontent.com/hazelcast/hazelcast/master/kubernetes-rbac.yaml`");
                isKnownExceptionAlreadyLogged = true;
            }
        } else {
            throw e;
        }
        LOGGER.finest(e);
        return emptyList();
    }

    private static JsonArray toJsonArray(JsonValue jsonValue) {
        if (jsonValue == null || jsonValue.isNull()) {
            return new JsonArray();
        } else {
            return jsonValue.asArray();
        }
    }

    private static String toString(JsonValue jsonValue) {
        if (jsonValue == null || jsonValue.isNull()) {
            return null;
        } else if (jsonValue.isString()) {
            return jsonValue.asString();
        } else {
            return jsonValue.toString();
        }
    }

    /**
     * Result which stores the information about a single endpoint.
     */
    static final class Endpoint {
        private final EndpointAddress privateAddress;
        private final EndpointAddress publicAddress;
        private final boolean isReady;
        private final Map<String, String> additionalProperties;

        Endpoint(EndpointAddress privateAddress, boolean isReady) {
            this.privateAddress = privateAddress;
            this.publicAddress = null;
            this.isReady = isReady;
            this.additionalProperties = Collections.emptyMap();
        }

        Endpoint(EndpointAddress privateAddress, boolean isReady, Map<String, String> additionalProperties) {
            this.privateAddress = privateAddress;
            this.publicAddress = null;
            this.isReady = isReady;
            this.additionalProperties = additionalProperties;
        }

        Endpoint(EndpointAddress privateAddress, EndpointAddress publicAddress, boolean isReady,
                 Map<String, String> additionalProperties) {
            this.privateAddress = privateAddress;
            this.publicAddress = publicAddress;
            this.isReady = isReady;
            this.additionalProperties = additionalProperties;
        }

        EndpointAddress getPublicAddress() {
            return publicAddress;
        }

        EndpointAddress getPrivateAddress() {
            return privateAddress;
        }

        boolean isReady() {
            return isReady;
        }

        Map<String, String> getAdditionalProperties() {
            return additionalProperties;
        }
    }

    static final class EndpointAddress {
        private final String ip;
        private final Integer port;

        EndpointAddress(String ip, Integer port) {
            this.ip = ip;
            this.port = port;
        }

        String getIp() {
            return ip;
        }

        Integer getPort() {
            return port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            EndpointAddress address = (EndpointAddress) o;

            if (ip != null ? !ip.equals(address.ip) : address.ip != null) {
                return false;
            }
            return port != null ? port.equals(address.port) : address.port == null;
        }

        @Override
        public int hashCode() {
            int result = ip != null ? ip.hashCode() : 0;
            result = 31 * result + (port != null ? port.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return String.format("%s:%s", ip, port);
        }
    }
}
