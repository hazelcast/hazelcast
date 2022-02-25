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

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.kubernetes.KubernetesClient.Endpoint;
import static com.hazelcast.kubernetes.KubernetesClient.EndpointAddress;
import static java.util.Arrays.asList;

interface KubernetesApiProvider {
    String getEndpointsByServiceLabelUrlString();

    String getEndpointsByNameUrlString();

    String getEndpointsUrlString();

    List<Endpoint> parseEndpointsList(JsonObject jsonObject);

    List<Endpoint> parseEndpoints(JsonValue jsonValue);

    Map<EndpointAddress, String> extractServices(JsonObject jsonObject,
                                                 List<EndpointAddress> addresses);

    Map<EndpointAddress, String> extractNodes(JsonObject jsonObject,
                                              List<EndpointAddress> addresses);

    default Integer extractPort(JsonValue subsetJson) {
        JsonArray ports = toJsonArray(subsetJson.asObject().get("ports"));
        for (JsonValue port : ports) {
            JsonValue hazelcastServicePort = port.asObject().get("name");
            if (hazelcastServicePort != null && hazelcastServicePort.asString().equals("hazelcast-service-port")) {
                JsonValue servicePort = port.asObject().get("port");
                if (servicePort != null && servicePort.isNumber()) {
                    return servicePort.asInt();
                }
            }
        }
        if (ports.size() == 1) {
            JsonValue port = ports.get(0);
            return port.asObject().get("port").asInt();
        }
        return null;
    }

    default Map<String, String> extractAdditionalPropertiesFrom(JsonValue endpointAddressJson) {
        Set<String> knownFieldNames = new HashSet<>(
                asList("ip", "nodeName", "targetRef", "hostname"));

        Map<String, String> result = new HashMap<>();
        for (JsonObject.Member member : endpointAddressJson.asObject()) {
            if (!knownFieldNames.contains(member.getName())) {
                result.put(member.getName(), convertToString(member.getValue()));
            }
        }
        return result;
    }

    default KubernetesClientException noCorrespondingServicesException(Set<EndpointAddress> endpoints) {
        return new KubernetesClientException(String.format("Cannot expose externally, the following Hazelcast"
                                   + " member pods do not have corresponding Kubernetes services: %s", endpoints));
    }


    default KubernetesClientException noNodeNameAssignedException(Set<EndpointAddress> endpoints) {
        return new KubernetesClientException(String.format("Cannot expose externally, the following Hazelcast"
                                   + " member pods do not have corresponding Endpoint.nodeName value assigned: %s", endpoints));
    }

    static JsonArray toJsonArray(JsonValue jsonValue) {
        if (jsonValue == null || jsonValue.isNull()) {
            return new JsonArray();
        } else {
            return jsonValue.asArray();
        }
    }

    static String convertToString(JsonValue jsonValue) {
        if (jsonValue == null || jsonValue.isNull()) {
            return null;
        } else if (jsonValue.isString()) {
            return jsonValue.asString();
        } else {
            return jsonValue.toString();
        }
    }
}
