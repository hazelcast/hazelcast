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

package com.hazelcast.azure;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for connecting to the Azure Instance Metadata API.
 *
 * @see <a href="https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service">
 * Azure Instance Metadata Service</a>
 */
final class AzureMetadataApi {
    private static final String METADATA_ENDPOINT = "http://169.254.169.254";
    private static final String API_VERSION = "2018-02-01";
    private static final String RESOURCE = "https://management.azure.com";

    private final String endpoint;
    private final Map<String, String> metadata;

    AzureMetadataApi() {
        this.endpoint = METADATA_ENDPOINT;
        this.metadata = new HashMap<String, String>();
    }

    /**
     * For test purposes only.
     */
    AzureMetadataApi(String endpoint, Map<String, String> metadata) {
        this.endpoint = endpoint;
        this.metadata = Collections.unmodifiableMap(metadata);
    }

    private void fillMetadata() {
        if (metadata.isEmpty()) {
            String urlString = String.format("%s/metadata/instance/compute?api-version=%s", endpoint, API_VERSION);
            String response = callGet(urlString);
            JsonObject jsonObject = Json.parse(response).asObject();
            for (String property : jsonObject.names()) {
                metadata.put(property, jsonObject.get(property).asString());
            }
        }
    }

    private String getMetadataProperty(String property) {
        fillMetadata();
        return metadata.get(property);
    }

    String subscriptionId() {
        return getMetadataProperty("subscriptionId");
    }

    String resourceGroupName() {
        return getMetadataProperty("resourceGroupName");
    }

    String location() {
        return getMetadataProperty("location");
    }

    String availabilityZone() {
        return getMetadataProperty("zone");
    }

    String faultDomain() {
        return getMetadataProperty("platformFaultDomain");
    }

    String scaleSet() {
        return getMetadataProperty("vmScaleSetName");
    }

    String accessToken() {
        String urlString = String.format("%s/metadata/identity/oauth2/token?api-version=%s&resource=%s", endpoint,
                API_VERSION, RESOURCE);
        String accessTokenResponse = callGet(urlString);
        return extractAccessToken(accessTokenResponse);
    }

    private String extractAccessToken(String accessTokenResponse) {
        return Json.parse(accessTokenResponse).asObject().get("access_token").asString();
    }

    private String callGet(String urlString) {
        return RestClient.create(urlString)
                .withHeader("Metadata", "true")
                .get();
    }

}
