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

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.kubernetes.KubernetesApiProvider.convertToString;
import static com.hazelcast.kubernetes.KubernetesApiProvider.toJsonArray;
import static com.hazelcast.kubernetes.KubernetesApiProvider.extractTargetRefName;
import static com.hazelcast.kubernetes.KubernetesClient.Endpoint;
import static com.hazelcast.kubernetes.KubernetesClient.EndpointAddress;

class KubernetesApiEndpointProvider
        implements KubernetesApiProvider {

    public String getEndpointsByServiceLabelUrlString() {
        return "%s/api/v1/namespaces/%s/endpoints?%s";
    }

    public String getEndpointsByNameUrlString() {
        return "%s/api/v1/namespaces/%s/endpoints/%s";
    }

    public String getEndpointsUrlString() {
        return "%s/api/v1/namespaces/%s/endpoints";
    }

    public List<Endpoint> parseEndpointsList(JsonObject endpointsListJson) {
        List<Endpoint> endpoints = new ArrayList<>();
        for (JsonValue item : toJsonArray(endpointsListJson.get("items"))) {
            endpoints.addAll(parseEndpoints(item));
        }
        return endpoints;
    }

    public List<Endpoint> parseEndpoints(JsonValue endpointItemJson) {
        List<Endpoint> addresses = new ArrayList<>();

        for (JsonValue subset : toJsonArray(endpointItemJson.asObject().get("subsets"))) {
            Integer endpointPort = extractPort(subset);
            for (JsonValue address : toJsonArray(subset.asObject().get("addresses"))) {
                addresses.add(extractEndpointAddress(address, endpointPort, true));
            }
            for (JsonValue address : toJsonArray(subset.asObject().get("notReadyAddresses"))) {
                addresses.add(extractEndpointAddress(address, endpointPort, false));
            }
        }
        return addresses;
    }

    private Endpoint extractEndpointAddress(JsonValue endpointAddressJson, Integer endpointPort, boolean isReady) {
        String ip = endpointAddressJson.asObject().get("ip").asString();
        String targetRefName = extractTargetRefName(endpointAddressJson);
        Map<String, String> additionalProperties = extractAdditionalPropertiesFrom(endpointAddressJson);
        return new Endpoint(new EndpointAddress(ip, endpointPort, targetRefName), isReady, additionalProperties);
    }

    public Map<EndpointAddress, String> extractServices(JsonObject endpointsListJson,
                                                        List<String> privateAddresses) {
        Map<EndpointAddress, String> result = new HashMap<>();
        Set<String> left = new HashSet<>(privateAddresses);
        for (JsonValue item : toJsonArray(endpointsListJson.get("items"))) {
            String service = convertToString(item.asObject().get("metadata").asObject().get("name"));
            List<Endpoint> endpoints = parseEndpoints(item);

            // Service must point to exactly one endpoint address, otherwise the public IP would be ambiguous.
            if (endpoints.size() == 1) {
                EndpointAddress address = endpoints.get(0).getPrivateAddress();
                // Omit the endpoint if the targetRef name in its private address is null.
                if (address.getTargetRefName() == null) {
                    continue;
                }
                if (privateAddresses.contains(address.getIp())) {
                    // If multiple services match the pod, then match service and pod names
                    if (!result.containsKey(address) || service.equals(address.getTargetRefName())) {
                        result.put(address, service);
                    }
                    left.remove(address.getIp());
                }
            }
        }
        if (!left.isEmpty()) {
            // At least one Hazelcast Member POD does not have a corresponding service.
            throw noCorrespondingServicesException(left);
        }
        return result;
    }

    public Map<EndpointAddress, String> extractNodes(JsonObject endpointsListJson,
                                                     List<String> privateAddresses) {
        Map<EndpointAddress, String> result = new HashMap<>();
        Set<String> left = new HashSet<>(privateAddresses);
        for (JsonValue item : toJsonArray(endpointsListJson.get("items"))) {
            for (JsonValue subset : toJsonArray(item.asObject().get("subsets"))) {
                JsonObject subsetObject = subset.asObject();
                List<Integer> ports = new ArrayList<>();
                for (JsonValue port : toJsonArray(subsetObject.get("ports"))) {
                    ports.add(port.asObject().get("port").asInt());
                }

                Map<EndpointAddress, String> nodes = new HashMap<>();
                nodes.putAll(extractNodes(subsetObject.get("addresses"), ports));
                nodes.putAll(extractNodes(subsetObject.get("notReadyAddresses"), ports));
                for (Map.Entry<EndpointAddress, String> nodeEntry : nodes.entrySet()) {
                    EndpointAddress address = nodeEntry.getKey();
                    if (privateAddresses.contains(address.getIp())) {
                        result.put(address, nodes.get(address));
                        left.remove(address.getIp());
                    }
                }
            }
        }
        if (!left.isEmpty()) {
            // At least one Hazelcast Member POD does not have 'nodeName' assigned.
            throw noNodeNameAssignedException(left);
        }
        return result;
    }

    private Map<EndpointAddress, String> extractNodes(JsonValue addressesJson, List<Integer> ports) {
        Map<EndpointAddress, String> result = new HashMap<>();
        for (JsonValue address : toJsonArray(addressesJson)) {
            String ip = address.asObject().get("ip").asString();
            String targetRefName = extractTargetRefName(address);
            String nodeName = KubernetesApiProvider.convertToString(address.asObject().get("nodeName"));
            for (Integer port : ports) {
                result.put(new EndpointAddress(ip, port, targetRefName), nodeName);
            }
        }
        return result;
    }

}
