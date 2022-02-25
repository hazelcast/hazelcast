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

package com.hazelcast.azure;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.spi.utils.RestClient;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * Responsible for connecting to the Azure Cloud Compute API.
 *
 * @see <a href="https://docs.microsoft.com/en-us/rest/api/compute/">Azure Compute API</a>
 */
class AzureComputeApi {
    static final String API_VERSION = "2018-08-01";
    static final String API_VERSION_SCALE_SET = "2018-06-01";
    private static final String AZURE_API_ENDPOINT = "https://management.azure.com";

    private final String endpoint;

    AzureComputeApi() {
        this.endpoint = AZURE_API_ENDPOINT;
    }

    /**
     * For test purposes only.
     */
    AzureComputeApi(String endpoint) {
        this.endpoint = endpoint;
    }

    Collection<AzureAddress> instances(String subscriptionId, String resourceGroup, String scaleSet,
                                       Tag tag, String accessToken) {
        String privateIpResponse = RestClient
                .create(urlForPrivateIpList(subscriptionId, resourceGroup, scaleSet))
                .withHeader("Authorization", String.format("Bearer %s", accessToken))
                .get()
                .getBody();

        Map<String, AzureNetworkInterface> networkInterfaces = parsePrivateIpResponse(privateIpResponse);

        String publicIpResponse = RestClient
                .create(urlForPublicIpList(subscriptionId, resourceGroup, scaleSet))
                .withHeader("Authorization", String.format("Bearer %s", accessToken))
                .get()
                .getBody();

        Map<String, String> publicIpMap = parsePublicIpResponse(publicIpResponse);

        Set<AzureAddress> addresses = new LinkedHashSet<>(networkInterfaces.size());

        for (AzureNetworkInterface anInterface : networkInterfaces.values()) {
            if (tag == null || anInterface.hasTag(tag)) {
                addresses.add(new AzureAddress(anInterface.getPrivateIp(), publicIpMap.get(anInterface.getPublicIpId())));
            }
        }

        return addresses;
    }

    private String urlForPrivateIpList(String subscriptionId, String resourceGroup, String scaleSet) {
        if (isNullOrEmptyAfterTrim(scaleSet)) {
            return String.format("%s/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network"
                    + "/networkInterfaces?api-version=%s", endpoint, subscriptionId, resourceGroup, API_VERSION);
        } else {
            return String.format("%s/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute"
                            + "/virtualMachineScaleSets/%s/networkInterfaces?api-version=%s",
                    endpoint, subscriptionId, resourceGroup, scaleSet, API_VERSION_SCALE_SET);
        }
    }

    private Map<String, AzureNetworkInterface> parsePrivateIpResponse(String response) {
        Map<String, AzureNetworkInterface> interfaces = new HashMap<>();

        for (JsonValue item : toJsonArray(Json.parse(response).asObject().get("value"))) {
            Set<Tag> tagList = new HashSet<>();
            JsonObject tags = toJsonObject(item.asObject().get("tags"));
            for (String key : tags.asObject().names()) {
                tagList.add(new Tag(key, tags.asObject().getString(key, null)));
            }

            JsonObject properties = item.asObject().get("properties").asObject();
            if (properties.get("virtualMachine") != null) {
                for (JsonValue ipConfiguration : toJsonArray(properties.get("ipConfigurations"))) {
                    JsonObject ipProps = ipConfiguration.asObject().get("properties").asObject();
                    String privateIp = ipProps.getString("privateIPAddress", null);
                    String publicIpId = toJsonObject(ipProps.get("publicIPAddress")).getString("id", null);
                    if (!isNullOrEmptyAfterTrim(privateIp)) {
                        interfaces.put(privateIp, new AzureNetworkInterface(privateIp, publicIpId, tagList));
                    }
                }
            }
        }
        return interfaces;
    }

    private String urlForPublicIpList(String subscriptionId, String resourceGroup, String scaleSet) {
        if (isNullOrEmptyAfterTrim(scaleSet)) {
            return String.format("%s/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network"
                    + "/publicIPAddresses?api-version=%s", endpoint, subscriptionId, resourceGroup, API_VERSION);
        } else {
            return String.format("%s/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute"
                            + "/virtualMachineScaleSets/%s/publicIPAddresses?api-version=%s",
                    endpoint, subscriptionId, resourceGroup, scaleSet, API_VERSION_SCALE_SET);
        }
    }

    private Map<String, String> parsePublicIpResponse(String response) {
        Map<String, String> publicIps = new HashMap<>();

        for (JsonValue item : toJsonArray(Json.parse(response).asObject().get("value"))) {
            String id = item.asObject().getString("id", null);
            String ip = toJsonObject(item.asObject().get("properties")).getString("ipAddress", null);
            if (!isNullOrEmptyAfterTrim(ip)) {
                publicIps.put(id, ip);
            }
        }

        return publicIps;
    }

    private static JsonArray toJsonArray(JsonValue jsonValue) {
        if (jsonValue == null || jsonValue.isNull()) {
            return new JsonArray();
        } else {
            return jsonValue.asArray();
        }
    }

    private static JsonObject toJsonObject(JsonValue jsonValue) {
        if (jsonValue == null || jsonValue.isNull()) {
            return new JsonObject();
        } else {
            return jsonValue.asObject();
        }
    }
}
