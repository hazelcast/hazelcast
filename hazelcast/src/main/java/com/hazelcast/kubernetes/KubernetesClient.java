/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.util.HostnameUtil;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.kubernetes.KubernetesConfig.ExposeExternallyMode;
import com.hazelcast.kubernetes.RuntimeContext.StatefulSetInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.exception.RestClientException;
import com.hazelcast.spi.utils.RestClient;
import com.hazelcast.spi.utils.RestClient.WatchResponse;
import com.hazelcast.spi.utils.RetryUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.StreamSupport;

import static com.hazelcast.instance.impl.ClusterTopologyIntentTracker.UNKNOWN;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_GONE;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Responsible for connecting to the Kubernetes API.
 *
 * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/">
 *      Kubernetes API</a>
 */
class KubernetesClient {
    static final String SERVICE_TYPE_LOADBALANCER = "LoadBalancer";
    static final String SERVICE_TYPE_NODEPORT = "NodePort";

    private static final ILogger LOGGER = Logger.getLogger(KubernetesClient.class);

    private static final int CONNECTION_TIMEOUT_SECONDS = 10;
    private static final int READ_TIMEOUT_SECONDS = 10;

    private static final List<String> NON_RETRYABLE_KEYWORDS = List.of(
            "\"reason\":\"Forbidden\"",
            "\"reason\":\"NotFound\"",
            "Failure in generating SSLSocketFactory",
            "REST call interrupted");

    private static final int STS_MONITOR_SHUTDOWN_AWAIT_TIMEOUT_MS = 1000;

    @Nullable
    final StsMonitorThread stsMonitorThread;
    private final String namespace;
    private final String kubernetesMaster;
    private final String caCertificate;
    private final int retries;
    private final KubernetesApiProvider apiProvider;
    private final ExposeExternallyMode exposeExternallyMode;
    private final boolean useNodeNameAsExternalAddress;
    private final String servicePerPodLabelName;
    private final String servicePerPodLabelValue;

    private final KubernetesTokenProvider tokenProvider;

    @Nullable
    private final ClusterTopologyIntentTracker clusterTopologyIntentTracker;

    private boolean isNoPublicIpAlreadyLogged;
    private boolean isKnownExceptionAlreadyLogged;
    private boolean isNodePortWarningAlreadyLogged;

    KubernetesClient(KubernetesConfig config, @Nullable ClusterTopologyIntentTracker clusterTopologyIntentTracker) {
        this(config.getNamespace(), HostnameUtil.getLocalHostname(), config.getKubernetesMasterUrl(), config.getTokenProvider(),
                config.getKubernetesCaCertificate(), config.getKubernetesApiRetries(), config.getExposeExternallyMode(),
                config.isUseNodeNameAsExternalAddress(), config.getServicePerPodLabelName(),
                config.getServicePerPodLabelValue(), clusterTopologyIntentTracker, null);
    }

    // test usage only
    @SuppressWarnings("ParameterNumber")
    KubernetesClient(String namespace, String podName, String kubernetesMaster,
                     KubernetesTokenProvider tokenProvider, String caCertificate, int retries,
                     ExposeExternallyMode exposeExternallyMode, boolean useNodeNameAsExternalAddress,
                     String servicePerPodLabelName, String servicePerPodLabelValue,
                     @Nullable ClusterTopologyIntentTracker clusterTopologyIntentTracker,
                     @Nullable KubernetesApiProvider apiProvider) {
        this.namespace = namespace;
        this.kubernetesMaster = kubernetesMaster;
        this.tokenProvider = tokenProvider;
        this.caCertificate = caCertificate;
        this.retries = retries;
        this.exposeExternallyMode = exposeExternallyMode;
        this.useNodeNameAsExternalAddress = useNodeNameAsExternalAddress;
        this.servicePerPodLabelName = servicePerPodLabelName;
        this.servicePerPodLabelValue = servicePerPodLabelValue;
        this.clusterTopologyIntentTracker = clusterTopologyIntentTracker;
        if (clusterTopologyIntentTracker != null) {
            clusterTopologyIntentTracker.initialize();
        }
        this.apiProvider = apiProvider != null ? apiProvider : buildKubernetesApiUrlProvider();
        this.stsMonitorThread = clusterTopologyIntentTracker != null && clusterTopologyIntentTracker.isEnabled()
                ? new StsMonitorThread(podName) : null;
    }

    public void start() {
        if (stsMonitorThread != null) {
            stsMonitorThread.start();
        }
    }

    public void destroy() {
        // It's important we shut down the StsMonitorThread first, as the ClusterTopologyIntentTracker
        // receives messages from this thread, and we want to let it process all available messages
        // before the intent tracker is shutdown
        if (stsMonitorThread != null) {
            LOGGER.info("Shutting down StatefulSet monitor thread");
            stsMonitorThread.shutdown();
        }

        if (clusterTopologyIntentTracker != null) {
            // Join the StsMonitor thread to ensure it has completed processing all messages
            // before shutting down our ClusterTopologyIntentTracker (which processes messages)
            if (stsMonitorThread != null) {
                try {
                    stsMonitorThread.join(STS_MONITOR_SHUTDOWN_AWAIT_TIMEOUT_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            clusterTopologyIntentTracker.destroy();
        }
    }

    KubernetesApiProvider buildKubernetesApiUrlProvider() {
        try {
            String endpointSlicesUrlString =
                    String.format("%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices", kubernetesMaster, namespace);
            callGet(endpointSlicesUrlString);
            LOGGER.finest("Using EndpointSlices API to discover endpoints.");
            return new KubernetesApiEndpointSlicesProvider();
        } catch (Exception e) {
            LOGGER.finest("EndpointSlices are not available, using Endpoints API to discover endpoints.");
            return new KubernetesApiEndpointProvider();
        }
    }

    /**
     * Retrieves POD addresses in the specified {@code namespace}.
     * @return all POD addresses
     *
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-143">
     *      Kubernetes Endpoint API</a>
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
     * Retrieves POD addresses for all services in the specified {@code namespace} filtered by
     * {@code serviceLabels} and {@code serviceLabelValues}.
     *
     * @param serviceLabels      comma separated labels used to filter responses
     * @param serviceLabelValues comma separated label values used to filter responses
     * @return all POD addresses from the specified {@code namespace} filtered by the labels
     *
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-143">
     *      Kubernetes Endpoint API</a>
     */
    List<Endpoint> endpointsByServiceLabel(String serviceLabels, String serviceLabelValues) {
        try {
            String param = getLabelSelectorParameter(serviceLabels, serviceLabelValues);
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
     *
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-143">
     *      Kubernetes Endpoint API</a>
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
     * Retrieves POD addresses for all services in the specified {@code namespace} filtered by
     * {@code podLabels} and {@code podLabelValues}.
     *
     * @param podLabels      comma separated labels used to filter responses
     * @param podLabelValues comma separated label values used to filter responses
     * @return all POD addresses from the specified {@code namespace} filtered by the labels
     *
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#list-143">
     *      Kubernetes Endpoint API</a>
     */
    List<Endpoint> endpointsByPodLabel(String podLabels, String podLabelValues) {
        try {
            String param = getLabelSelectorParameter(podLabels, podLabelValues);
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
     *
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11">
     *      Kubernetes Endpoint API</a>
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
     *
     * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11">
     *      Kubernetes Endpoint API</a>
     */
    String nodeName(String podName) {
        String podUrlString = String.format("%s/api/v1/namespaces/%s/pods/%s", kubernetesMaster, namespace, podName);
        return extractNodeName(callGet(podUrlString));
    }

    // For test purpose
    boolean isNoPublicIpAlreadyLogged() {
        return isNoPublicIpAlreadyLogged;
    }

    // For test purpose
    boolean isKnownExceptionAlreadyLogged() {
        return isKnownExceptionAlreadyLogged;
    }

    // For test purpose
    boolean isNodePortWarningAlreadyLogged() {
        return isNodePortWarningAlreadyLogged;
    }

    @Nullable
    private String extractNodeName(EndpointAddress endpointAddress, Map<EndpointAddress, String> nodes) {
        String nodeName = nodes.get(endpointAddress);
        if (nodeName == null) {
            JsonObject podJson = callGet(String.format("%s/api/v1/namespaces/%s/pods/%s",
                    kubernetesMaster, namespace, endpointAddress.getTargetRefName()));
            return podJson.get("spec").asObject().get("nodeName").asString();
        }
        return nodeName;
    }

    /**
     * Tries to add public addresses to the endpoints.
     * <p>
     * If it's not possible, then returns the input parameter.
     * <p>
     * Assigning public IPs must meet one of the following requirements: <ul>
     * <li> Each POD must be exposed with a separate LoadBalancer service OR
     * <li> Each POD must be exposed with a separate NodePort service and Kubernetes nodes must have external IPs
     * </ul><p>
     * The algorithm to fetch public IPs is as follows: <ol>
     * <li> Use Kubernetes API (/endpoints) to find dedicated services for each POD
     * <li> For each POD: <ul>
     *      <li> If the corresponding service type is LoadBalancer, it extracts the External IP and Service Port
     *      <li> If the service type is NodePort, it uses the Kubernetes API (/nodes) to find the External IP of the Node
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

            List<String> privateAddresses = privateAddresses(endpoints);
            Map<EndpointAddress, String> services = apiProvider.extractServices(endpointsJson, privateAddresses);
            Map<EndpointAddress, String> nodeAddresses = apiProvider.extractNodes(endpointsJson, privateAddresses);

            Map<String, Address> publicServiceAddresses = new HashMap<>();
            Map<String, String> cachedNodePublicIps = new HashMap<>();

            for (Map.Entry<EndpointAddress, String> serviceEntry : services.entrySet()) {
                EndpointAddress privateAddress = serviceEntry.getKey();
                String service = serviceEntry.getValue();
                String serviceUrl = String.format("%s/api/v1/namespaces/%s/services/%s", kubernetesMaster, namespace, service);
                JsonObject serviceJson = callGet(serviceUrl);
                String serviceType = extractServiceType(serviceJson);

                if (SERVICE_TYPE_LOADBALANCER.equals(serviceType)) {
                    Address loadBalancerServiceAddress = extractLoadBalancerServiceAddress(serviceJson);
                    publicServiceAddresses.put(privateAddress.getIp(), loadBalancerServiceAddress);
                } else if (SERVICE_TYPE_NODEPORT.equals(serviceType)) {
                    Address nodePortServiceAddress = extractNodePortServiceAddress(serviceJson, serviceEntry.getKey(),
                            nodeAddresses, cachedNodePublicIps);
                    publicServiceAddresses.put(privateAddress.getIp(), nodePortServiceAddress);
                    // Log warning only once.
                    if (!isNodePortWarningAlreadyLogged && exposeExternallyMode == ExposeExternallyMode.ENABLED) {
                        LOGGER.warning("Using NodePort service type for public addresses may lead to connection issues from "
                                + "outside of the Kubernetes cluster. Ensure external accessibility of the NodePort IPs.");
                        isNodePortWarningAlreadyLogged = true;
                    }
                } else {
                    throw new IllegalStateException(String.format(
                            "Service type '%s' is not supported to discover the public addresses of the members", serviceType));
                }
            }

            return createEndpoints(endpoints, publicServiceAddresses);
        } catch (Exception e) {
            if (exposeExternallyMode == ExposeExternallyMode.ENABLED) {
                throw e;
            }
            // If expose-externally not set (exposeExternallyMode == ExposeExternallyMode.AUTO),
            // silently ignore any exception
            LOGGER.finest(e);
            // Log warning only once.
            if (!isNoPublicIpAlreadyLogged) {
                LOGGER.warning("Cannot fetch public IPs of Hazelcast Member PODs, you won't be able to use "
                        + "Hazelcast MULTI_MEMBER or ALL_MEMBERS routing Clients from outside of the Kubernetes network");
                isNoPublicIpAlreadyLogged = true;
            }
            return endpoints;
        }
    }

    private String externalIpAddressForNode(String node) {
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

    private Address extractNodePortServiceAddress(JsonObject serviceJson, EndpointAddress endpointAddress,
                                                  Map<EndpointAddress, String> nodeAddresses,
                                                  Map<String, String> cachedNodePublicIps) {
        Integer nodePort = extractNodePort(serviceJson);
        String node = extractNodeName(endpointAddress, nodeAddresses);
        String nodePublicIpAddress;
        if (cachedNodePublicIps.containsKey(node)) {
            nodePublicIpAddress = cachedNodePublicIps.get(node);
        } else {
            nodePublicIpAddress = externalIpAddressForNode(node);
            cachedNodePublicIps.put(node, nodePublicIpAddress);
        }
        return new Address(nodePublicIpAddress, nodePort);
    }

    /**
     * Makes a REST call to Kubernetes API and returns the result JSON.
     *
     * @param urlString Kubernetes API REST endpoint
     * @return parsed JSON
     * @throws KubernetesClientException if Kubernetes API didn't respond with 200 and a valid JSON content
     */
    private JsonObject callGet(final String urlString) {
        return RetryUtils.retry(() ->
                Json.parse((caCertificate == null ? RestClient.create(urlString, CONNECTION_TIMEOUT_SECONDS)
                                : RestClient.createWithSSL(urlString, caCertificate, CONNECTION_TIMEOUT_SECONDS))
                        .withHeader("Authorization", "Bearer " + tokenProvider.getToken())
                        .withRequestTimeoutSeconds(READ_TIMEOUT_SECONDS)
                        .get()
                        .getBody()
                ).asObject(), retries, NON_RETRYABLE_KEYWORDS);
    }

    private List<Endpoint> handleKnownException(RestClientException e) {
        if (e.getHttpErrorCode() == HTTP_UNAUTHORIZED) {
            if (!isKnownExceptionAlreadyLogged) {
                LOGGER.warning("Kubernetes API authorization failure! To use Hazelcast Kubernetes discovery, "
                        + "please check your 'api-token' property. Starting standalone.");
                isKnownExceptionAlreadyLogged = true;
            }
        } else if (e.getHttpErrorCode() == HTTP_FORBIDDEN) {
            if (!isKnownExceptionAlreadyLogged) {
                LOGGER.warning("Kubernetes API access is forbidden! Starting standalone. To use Hazelcast Kubernetes discovery, "
                        + "configure the required RBAC. For 'default' service account in 'default' namespace execute "
                        + "`kubectl apply -f https://raw.githubusercontent.com/hazelcast/hazelcast/master/kubernetes-rbac.yaml` "
                        + "If you want to use a different service account and a different namespace, "
                        + "you can update the mentioned rbac.yaml file accordingly and use it. "
                        + "Error Kubernetes API Cause details:", e);
                isKnownExceptionAlreadyLogged = true;
            }
        } else {
            throw e;
        }
        LOGGER.finest(e);
        return emptyList();
    }

    private static String getLabelSelectorParameter(String labelNames, String labelValues) {
        String[] labelNameList = labelNames.split(",");
        String[] labelValueList = labelValues.split(",");
        List<String> selectorList = new ArrayList<>(labelNameList.length);
        for (int i = 0; i < labelNameList.length; i++) {
            selectorList.add(labelNameList[i] + "=" + labelValueList[i]);
        }
        return "labelSelector=" + String.join(",", selectorList);
    }

    private static List<Endpoint> parsePodsList(JsonObject podsListJson) {
        List<Endpoint> addresses = new ArrayList<>();
        for (JsonValue item : toJsonArray(podsListJson.get("items"))) {
            String podName = item.asObject().get("metadata").asObject().get("name").asString();
            JsonObject status = item.asObject().get("status").asObject();
            String ip = toString(status.get("podIP"));
            if (ip != null) {
                Integer port = extractContainerPort(item);
                addresses.add(new Endpoint(new EndpointAddress(ip, port, podName), isReady(status)));
            }
        }
        return addresses;
    }

    private static Integer extractContainerPort(JsonValue podItemJson) {
        JsonArray containers = toJsonArray(podItemJson.asObject().get("spec").asObject().get("containers"));
        // If multiple containers are in one POD, then use the default Hazelcast port from the configuration.
        if (containers.size() == 1) {
            JsonValue container = containers.get(0);
            return containerPort(container);
        } else {
            for (JsonValue container : containers) {
                if (container.asObject().getString("name", "").equals("hazelcast")) {
                    return containerPort(container);
                }
            }
        }
        return null;
    }

    private static Integer containerPort(JsonValue container) {
        JsonArray ports = toJsonArray(container.asObject().get("ports"));
        // If multiple ports are exposed by a container, then use the default Hazelcast port from the configuration.
        if (!ports.isEmpty()) {
            JsonValue port = ports.get(0);
            JsonValue containerPort = port.asObject().get("containerPort");
            if (containerPort != null && containerPort.isNumber()) {
                return containerPort.asInt();
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
        List<String> zoneLabels = List.of("topology.kubernetes.io/zone", "failure-domain.kubernetes.io/zone",
                "failure-domain.beta.kubernetes.io/zone");
        for (String zoneLabel : zoneLabels) {
            JsonValue zone = labels.get(zoneLabel);
            if (zone != null) {
                return toString(zone);
            }
        }
        return null;
    }

    private static String extractServiceType(JsonObject serviceResponse) {
        return serviceResponse.get("spec").asObject().get("type").asString();
    }

    private static Address extractLoadBalancerServiceAddress(JsonObject serviceJson) {
        String loadBalancerIpAddress = extractLoadBalancerIpAddress(serviceJson);
        Integer servicePort = extractServicePort(serviceJson);
        return new Address(loadBalancerIpAddress, servicePort);
    }

    private static List<String> privateAddresses(List<Endpoint> endpoints) {
        List<String> result = new ArrayList<>();
        for (Endpoint endpoint : endpoints) {
            result.add(endpoint.getPrivateAddress().getIp());
        }
        return result;
    }

    private static String extractLoadBalancerIpAddress(JsonObject serviceResponse) {
        try {
            JsonObject ingress = serviceResponse
                    .get("status").asObject()
                    .get("loadBalancer").asObject()
                    .get("ingress").asArray().get(0).asObject();
            JsonValue address = ingress.get("ip");
            if (address == null) {
                address = ingress.get("hostname");
            }
            return address.asString();
        } catch (Exception e) {
            throw new KubernetesClientException("Unable to extract the public address from the LoadBalancer service", e);
        }
    }

    private static List<Endpoint> createEndpoints(List<Endpoint> endpoints, Map<String, Address> publicAddresses) {
        List<Endpoint> result = new ArrayList<>();
        for (Endpoint endpoint : endpoints) {
            EndpointAddress privateAddress = endpoint.getPrivateAddress();
            Address serviceAddress = publicAddresses.get(privateAddress.getIp());
            EndpointAddress publicAddress = new EndpointAddress(serviceAddress.ip, serviceAddress.port,
                    privateAddress.getTargetRefName());
            result.add(new Endpoint(privateAddress, publicAddress, endpoint.isReady(), endpoint.getAdditionalProperties()));
        }
        return result;
    }

    private static Integer extractServicePort(JsonObject serviceJson) {
        JsonArray ports = toJsonArray(serviceJson.get("spec").asObject().get("ports"));
        if (ports.size() == 1) {
            return ports.get(0).asObject().get("port").asInt();
        }
        for (JsonValue port : ports) {
            JsonValue servicePortName = port.asObject().get("name");
            if (servicePortName != null && servicePortName.asString().equals("hazelcast")) {
                return port.asObject().get("port").asInt();
            }
        }
        throw new KubernetesClientException(String.format("Cannot expose externally, service %s needs to have "
                    + "either exactly one port defined, or a port with 'hazelcast' name",
                serviceJson.get("metadata").asObject().get("name")));
    }

    private static Integer extractNodePort(JsonObject serviceJson) {
        JsonArray ports = toJsonArray(serviceJson.get("spec").asObject().get("ports"));
        if (ports.size() == 1) {
            return ports.get(0).asObject().get("nodePort").asInt();
        }
        for (JsonValue port: ports) {
            JsonValue servicePortName = port.asObject().get("name");
            if (servicePortName != null && servicePortName.asString().equals("hazelcast")) {
                return port.asObject().get("nodePort").asInt();
            }
        }
        throw new KubernetesClientException(String.format("Cannot expose externally, service %s needs to have "
                    + "either exactly one port defined, or a port with 'hazelcast' name",
                serviceJson.get("metadata").asObject().get("name")));
    }

    private static String extractNodePublicIp(JsonObject nodeJson) {
        for (JsonValue address : toJsonArray(nodeJson.get("status").asObject().get("addresses"))) {
            if ("ExternalIP".equals(address.asObject().get("type").asString())) {
                return address.asObject().get("address").asString();
            }
        }
        throw new KubernetesClientException(String.format("Cannot expose externally, node %s does not have"
                + " ExternalIP assigned", nodeJson.get("metadata").asObject().get("name")));
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
        private final Address address;

        private String targetRefName;

        EndpointAddress(Address address) {
            this.address = address;
        }

        EndpointAddress(String ip, Integer port) {
            this(new Address(ip, port));
        }

        EndpointAddress(String ip, Integer port, String targetRefName) {
            this(ip, port);
            this.targetRefName = targetRefName;
        }

        String getIp() {
            return address.ip;
        }

        Integer getPort() {
            return address.port;
        }

        String getTargetRefName() {
            return targetRefName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            EndpointAddress endpointAddress = (EndpointAddress) o;
            return Objects.equals(address, endpointAddress.address)
                    && Objects.equals(targetRefName, endpointAddress.targetRefName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, targetRefName);
        }

        @Override
        public String toString() {
            return address.toString();
        }
    }

    static final class Address {
        private final String ip;
        private final Integer port;

        Address(String ip, Integer port) {
            this.ip = ip;
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Address address = (Address) o;
            return Objects.equals(ip, address.ip) && Objects.equals(port, address.port);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ip, port);
        }

        @Override
        public String toString() {
            return ip + ":" + port;
        }
    }

    final class StsMonitorThread extends Thread {

        // backoff properties when retrying
        private static final int MAX_SPINS = 3;
        private static final int MAX_YIELDS = 10;
        private static final int MIN_PARK_PERIOD_MILLIS = 1;
        private static final int MAX_PARK_PERIOD_SECONDS = 10;

        // used only for tests
        volatile boolean running = true;
        volatile boolean finished;
        volatile boolean shuttingDown;

        RuntimeContext latestRuntimeContext;
        int idleCount;
        WatchResponse watchResponse;

        private final String stsUrlString;
        private final List<String> stsNames;
        private final BackoffIdleStrategy backoffIdleStrategy;

        StsMonitorThread(String podName) {
            super("hz-k8s-sts-monitor");
            stsUrlString = String.format("%s/apis/apps/v1/namespaces/%s/statefulsets", kubernetesMaster, namespace);
            stsNames = chooseStsToMonitor(podName, stsUrlString);
            backoffIdleStrategy = new BackoffIdleStrategy(MAX_SPINS, MAX_YIELDS, MILLISECONDS.toNanos(MIN_PARK_PERIOD_MILLIS),
                    SECONDS.toNanos(MAX_PARK_PERIOD_SECONDS));
        }

        private List<String> chooseStsToMonitor(String podName, String stsUrl) {
            String stsRootName = determineStsRootName(podName, stsUrl);
            return List.of(stsRootName, stsRootName + "-lite");
        }

        /**
         * The Hazelcast Platform Operator keeps instances in two StatefulSet's: sts-name and sts-name-lite. We
         * need to monitor both to detect cluster changes. We can resolve the name of the sts containing this member
         * easily since the pattern for the pod name (and therefore local hostname) is {@code <statefulSetName>-<ordinal>}
         * (see <a href="https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#stable-network-id">
         * here</a>). From this name we can then determine the name of the sibling sts by querying the api
         * server if required for disambiguation. For example if the current sts name is suffixed with "lite"
         * then this member may be a lite member, or the user chose a name with that suffix and we are
         * actually a standard member.
         */
        private String determineStsRootName(String localHostName, String stsUrl) {
            String parentStsName = localHostName.replaceAll("-\\d+$", "");
            if (!parentStsName.endsWith("lite")) {
                return parentStsName;
            }

            JsonArray items = toJsonArray(callGet(stsUrl).get("items"));
            List<String> stsNames = StreamSupport.stream(items.spliterator(), false)
                    .map(sts -> sts.asObject().get("metadata").asObject().getString("name", ""))
                    .toList();

            boolean inLiteSts = stsNames.stream().anyMatch(name -> parentStsName.equals(name + "-lite"));
            boolean inFullSts = stsNames.stream().anyMatch(name -> name.equals(parentStsName + "-lite"));

            if (inFullSts && inLiteSts) {
                throw new IllegalStateException("Cannot determine root sts name from: " + stsNames);
            } else if (inLiteSts) {
                return parentStsName.replaceAll("-lite$", "");
            } else {
                return parentStsName;
            }
        }

        /**
         * Initializes and watches information about the StatefulSets in which Hazelcast is being executed.
         * See <a href="https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes">
         * Efficient detection of changes on Kubernetes API reference</a>.
         * <p>
         * Important: If this thread starves, then timely updates may be stalled and shutdown hook
         * may not act on the latest cluster information.
         */
        @Override
        public void run() {
            LOGGER.info("Starting to monitor statefulsets: " + stsNames);
            while (running) {
                if (shuttingDown) {
                    break;
                }
                try {
                    // read initial StatefulSet list
                    RuntimeContext previous = latestRuntimeContext;
                    readInitialStsList();
                    // update tracker
                    updateTracker(previous, latestRuntimeContext);
                    watchResponse = sendWatchRequest();
                } catch (RestClientException e) {
                    // interrupts during shutdown will trigger a RestClientException
                    if (shuttingDown) {
                        break;
                    }
                    handleFailure(e);
                    // always retry after a RestClientException
                    continue;
                }
                // reset backoff-idle count
                idleCount = 0;
                try {
                    String message;
                    while ((message = watchResponse.nextLine()) != null) {
                        onWatchEventReceived(message);
                    }
                } catch (IOException e) {
                    // If we're shutting down, the watchResponse is already disconnected, and
                    // the IOException can be disregarded; otherwise continue with logging
                    if (!shuttingDown) {
                        LOGGER.info("Exception while watching for StatefulSet changes", e);

                        try {
                            watchResponse.disconnect();
                        } catch (Exception t) {
                            LOGGER.fine("Exception while closing connection after an IOException", t);
                        }
                    }
                }
            }
            finished = true;
        }

        public void shutdown() {
            this.shuttingDown = true;
            try {
                if (watchResponse != null) {
                    watchResponse.disconnect();
                }
            } catch (IOException e) {
                LOGGER.fine("Exception while closing connection during shutdown", e);
            }
            // Interrupt thread as we may be in the process of making watch requests
            // or other calls that need to be interrupted for us to shut down promptly
            interrupt();
        }

        private void handleFailure(RestClientException e) {
            if (e.getHttpErrorCode() == HTTP_GONE) {
                // occurs when the resource version we are watching for is stale
                LOGGER.info("StatefulSet watcher has fallen behind, re-reading sts list and resuming watch: "
                        + e.getMessage());
            } else {
                // watch failed with another HTTP error code, let's log at WARNING level,
                // backoff and try to resume again
                LOGGER.warning("Error while attempting to watch kubernetes API for StatefulSets: "
                        + e.getHttpErrorCode() + " " + e.getMessage() + ". Backing off (n: " + idleCount
                        + " ) before retrying.");
                backoffIdleStrategy.idle(idleCount);
                idleCount++;
            }
        }

        /**
         * GET StatefulSets list and update the latest runtime context.
         */
        void readInitialStsList() {
            JsonObject stsList = callGet(stsUrlString);
            String resourceVersion = stsList.get("metadata").asObject().getString("resourceVersion", null);

            latestRuntimeContext = new RuntimeContext();
            for (JsonValue statefulSet : toJsonArray(stsList.get("items"))) {
                String stsName = statefulSet.asObject().get("metadata").asObject().getString("name", null);
                if (stsNames.contains(stsName)) {
                    latestRuntimeContext.addStatefulSetInfo(stsName, StatefulSetInfo.from(statefulSet.asObject()),
                            resourceVersion);
                    if (latestRuntimeContext.getStatefulSetCount() == stsNames.size()) {
                        return;
                    }
                }
            }
        }

        /**
         * Send a watch request.
         * @return a {@link WatchResponse} that can be used to poll for watch events from Kubernetes API server
         */
        @Nonnull
        WatchResponse sendWatchRequest() throws RestClientException {
            RestClient restClient = (caCertificate == null ? RestClient.create(stsUrlString)
                            : RestClient.createWithSSL(stsUrlString, caCertificate))
                    .withHeader("Authorization", "Bearer " + tokenProvider.getToken());
            return restClient.watch(latestRuntimeContext.getResourceVersion());
        }

        /**
         * @see <a href="https://github.com/kubernetes/apimachinery/blob/master/pkg/watch/watch.go">
         *      Watch Event Specification</a>
         */
        void onWatchEventReceived(String message) {
            if (LOGGER.isFinestEnabled()) {
                LOGGER.finest("Complete message from kubernetes API: %s", message);
            }
            JsonObject watchEvent = Json.parse(message).asObject();
            JsonObject statefulSet = watchEvent.get("object").asObject();
            String stsName = statefulSet.asObject().get("metadata").asObject().getString("name", null);
            if (!stsNames.contains(stsName)) {
                return;
            }
            String watchType = watchEvent.getString("type", null);
            RuntimeContext context = null;
            switch (watchType) {
                case "ADDED", "MODIFIED", "DELETED":
                    String resourceVersion = statefulSet.get("metadata").asObject().getString("resourceVersion", null);
                    StatefulSetInfo info = StatefulSetInfo.from(statefulSet);
                    if (watchType.equals("DELETED")) {
                        info = new StatefulSetInfo(0, info.readyReplicas(), info.currentReplicas());
                    }
                    context = latestRuntimeContext != null ? new RuntimeContext(latestRuntimeContext)
                            : new RuntimeContext();
                    context.addStatefulSetInfo(stsName, info, resourceVersion);
                    break;
                default:
                    // BOOKMARK, ERROR
                    // Since we only listen to StatefulSet events, the client-side resourceVersion may be
                    // "forgotten" by the server if the cluster generates too many non-topological events.
                    // In this case, if the connection to the server is lost, specifying the client-side
                    // resourceVersion when reconnecting to the server results in a "too old version" error.
                    // Bookmarks are introduced to resolve this issue by generating events that consists of the
                    // latest resourceVersion only, which can be enabled by setting `allowWatchBookmarks=true`
                    // in the list & watch request. However, we do not make use of them currently.
                    LOGGER.info("Unknown watch type " + watchType + ", complete message:\n" + message);
            }
            if (latestRuntimeContext != null && context != null) {
                updateTracker(latestRuntimeContext, context);
            }
            latestRuntimeContext = context;
        }

        void updateTracker(RuntimeContext previous, RuntimeContext updated) {
            if (previous != null) {
                LOGGER.info("Updating cluster topology tracker with previous: " + previous + ", updated: " + updated);
                clusterTopologyIntentTracker.update(
                        previous.getSpecifiedReplicaCount(), updated.getSpecifiedReplicaCount(),
                        previous.getReadyReplicas(), updated.getReadyReplicas(),
                        previous.getCurrentReplicas(), updated.getCurrentReplicas());
            } else {
                LOGGER.info("Initializing cluster topology tracker with initial context: " + updated);
                clusterTopologyIntentTracker.update(
                        UNKNOWN, updated.getSpecifiedReplicaCount(),
                        UNKNOWN, updated.getReadyReplicas(),
                        UNKNOWN, updated.getCurrentReplicas());
            }
        }
    }
}
