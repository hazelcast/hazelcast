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
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.exception.NoCredentialsException;
import com.hazelcast.spi.utils.RestClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for connecting to the Azure Instance Metadata API.
 *
 * @see <a href="https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service">
 * Azure Instance Metadata Service</a>
 */
class AzureMetadataApi {
    static final String API_VERSION = "2018-02-01";
    static final String RESOURCE = "https://management.azure.com";
    private static final String METADATA_ENDPOINT = "http://169.254.169.254";
    private static final int HTTP_BAD_REQUEST = 400;

    private final String endpoint;
    private final Map<String, String> metadata;

    AzureMetadataApi() {
        this.endpoint = METADATA_ENDPOINT;
        this.metadata = new HashMap<>();
    }

    /**
     * For test purposes only.
     */
    AzureMetadataApi(String endpoint, Map<String, String> metadata) {
        this.endpoint = endpoint;
        this.metadata = metadata;
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

    private String getMetadataProperty(String property) {
        fillMetadata();
        return metadata.get(property);
    }

    private void fillMetadata() {
        if (metadata.isEmpty()) {
            String urlString = String.format("%s/metadata/instance/compute?api-version=%s", endpoint, API_VERSION);
            String response = callGet(urlString);
            JsonObject jsonObject = Json.parse(response).asObject();
            for (String property : jsonObject.names()) {
                if (jsonObject.get(property).isString()) {
                    metadata.put(property, jsonObject.get(property).asString());
                }
            }
        }
    }

    String accessToken() {
        try {
            String urlString = String.format("%s/metadata/identity/oauth2/token?api-version=%s&resource=%s", endpoint,
                    API_VERSION, RESOURCE);
            String accessTokenResponse = callGet(urlString);
            return extractAccessToken(accessTokenResponse);
        } catch (Exception e) {
            throw new NoCredentialsException("Error while fetching access token from Azure API using managed identity.", e);
        }
    }


    private String callGet(String urlString) {
        return RestClient.create(urlString)
                .withHeader("Metadata", "true")
                .get()
                .getBody();
    }

    private String extractAccessToken(String accessTokenResponse) {
        return Json.parse(accessTokenResponse).asObject().get("access_token").asString();
    }

}
