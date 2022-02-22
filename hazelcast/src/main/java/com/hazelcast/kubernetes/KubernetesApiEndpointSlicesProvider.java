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

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.hazelcast.kubernetes.KubernetesApiProvider.toJsonArray;
import static com.hazelcast.kubernetes.KubernetesApiProvider.convertToString;
import static com.hazelcast.kubernetes.KubernetesClient.Endpoint;
import static com.hazelcast.kubernetes.KubernetesClient.EndpointAddress;

public class KubernetesApiEndpointSlicesProvider
        implements KubernetesApiProvider {

    public String getEndpointsByServiceLabelUrlString() {
        return "%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices?%s";
    }

    public String getEndpointsByNameUrlString() {
        return "%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices?labelSelector=kubernetes.io/service-name=%s";
    }

    public String getEndpointsUrlString() {
        return "%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices";
    }

    public List<Endpoint> parseEndpointsList(JsonObject jsonObject) {
        List<Endpoint> endpoints = new ArrayList<>();
        for (JsonValue item : toJsonArray(jsonObject.get("items"))) {
            endpoints.addAll(parseEndpointSlices(item));
        }
        return endpoints;
    }

    public List<Endpoint> parseEndpoints(JsonValue jsonValue) {
        return parseEndpointsList(jsonValue.asObject());
    }

    public Map<EndpointAddress, String> extractServices(JsonObject endpointsListJson,
                                                        List<EndpointAddress> privateAddresses) {
        Map<EndpointAddress, String> result = new HashMap<>();
        Set<EndpointAddress> left = new HashSet<>(privateAddresses);
        for (JsonValue item : toJsonArray(endpointsListJson.get("items"))) {
            JsonValue ownerRefsValue = item.asObject().get("metadata").asObject().get("ownerReferences");
            if (ownerRefsValue == null || ownerRefsValue.asArray().size() > 1
                || !ownerRefsValue.asArray().get(0).asObject().get("kind").asString().equals("Service")) {
                continue;
            }
            String service = ownerRefsValue.asArray().get(0).asObject().get("name").asString();
            List<Endpoint> endpoints = parseEndpointSlices(item);

            // Service must point to exactly one endpoint address, otherwise the public IP would be ambiguous.
            if (endpoints.size() == 1) {
                EndpointAddress address = endpoints.get(0).getPrivateAddress();
                if (privateAddresses.contains(address)) {
                    // If multiple services match the pod, then match service and pod names
                    if (!result.containsKey(address) || service.equals(extractTargetRefName(item))) {
                        result.put(address, service);
                    }
                    left.remove(address);
                }
            }
        }
        if (!left.isEmpty()) {
            // At least one Hazelcast Member POD does not have a corresponding service.
            throw noCorrespondingServicesException(left);
        }
        return result;
    }

    private List<Endpoint> parseEndpointSlices(JsonValue jsonValue) {
        List<KubernetesClient.Endpoint> addresses = new ArrayList<>();
        Integer endpointPort = extractPort(jsonValue);

        for (JsonValue endpoint : toJsonArray(jsonValue.asObject().get("endpoints"))) {
            JsonValue ready = endpoint.asObject().get("conditions").asObject().get("ready");
            Map<String, String> additionalProperties = extractAdditionalPropertiesFrom(endpoint);
            for (JsonValue address : toJsonArray(endpoint.asObject().get("addresses"))) {
                addresses.add(new Endpoint(new EndpointAddress(address.asString(), endpointPort),
                        ready.asBoolean(), additionalProperties));
            }
        }
        return addresses;
    }

    private String extractTargetRefName(JsonValue endpointItemJson) {
        return Optional.of(endpointItemJson)
                       .flatMap(e -> toJsonArray(e.asObject().get("endpoints")).values().stream().findFirst())
                       .map(e -> e.asObject().get("targetRef"))
                       .map(e -> e.asObject().get("name"))
                       .map(KubernetesApiProvider::convertToString)
                       .orElse(null);
    }

    @Override
    public Map<EndpointAddress, String> extractNodes(JsonObject jsonObject,
                                                     List<EndpointAddress> privateAddresses) {
        Map<EndpointAddress, String> result = new HashMap<>();
        Set<EndpointAddress> left = new HashSet<>(privateAddresses);
        for (JsonValue item : toJsonArray(jsonObject.get("items"))) {
            List<Integer> ports = new ArrayList<>();
            for (JsonValue port : toJsonArray(item.asObject().get("ports"))) {
                ports.add(port.asObject().get("port").asInt());
            }
            for (JsonValue endpoint : toJsonArray(item.asObject().get("endpoints"))) {
                JsonObject endpointObject = endpoint.asObject();
                String nodeName = convertToString(endpointObject.get("nodeName"));

                Map<EndpointAddress, String> nodes = extractNodes(endpointObject.get("addresses"), ports, nodeName);
                for (Map.Entry<EndpointAddress, String> nodeEntry : nodes.entrySet()) {
                    EndpointAddress address = nodeEntry.getKey();
                    if (privateAddresses.contains(address)) {
                        result.put(address, nodes.get(address));
                        left.remove(address);
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

    private Map<EndpointAddress, String> extractNodes(JsonValue addressesJson, List<Integer> ports, String nodeName) {
        Map<EndpointAddress, String> result = new HashMap<>();
        for (JsonValue address : addressesJson.asArray()) {
            for (Integer port : ports) {
                result.put(new EndpointAddress(address.asString(), port), nodeName);
            }
        }
        return result;
    }
}
