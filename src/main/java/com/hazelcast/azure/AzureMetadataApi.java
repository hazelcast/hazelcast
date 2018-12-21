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

/**
 * Responsible for connecting to the Azure Instance Metadata API.
 *
 * @see <a href="https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service">
 * Azure Instance Metadata Service</a>
 */
final class AzureMetadataApi {
    private static final String METADATA_ENDPOINT = "http://169.254.169.254";
    private static final String API_VERSION = "2018-02-01";

    private final String endpoint;

    AzureMetadataApi() {
        this.endpoint = METADATA_ENDPOINT;
    }

    /**
     * For test purposes only.
     */
    AzureMetadataApi(String endpoint) {
        this.endpoint = endpoint;
    }

    String subscriptionId() {
        String urlString = String.format("%s/metadata/instance/compute/subscriptionId", endpoint);
        return callGet(urlString, true);
    }

    String resourceGroupName() {
        String urlString = String.format("%s/metadata/instance/compute/resourceGroupName", endpoint);
        return callGet(urlString, true);
    }

    String location() {
        String urlString = String.format("%s/metadata/instance/compute/location", endpoint);
        return callGet(urlString, true);
    }

    String availabilityZone() {
        String urlString = String.format("%s/metadata/instance/compute/zone", endpoint);
        return callGet(urlString, true);
    }

    String faultDomain() {
        String urlString = String.format("%s/metadata/instance/compute/platformFaultDomain", endpoint);
        return callGet(urlString, true);
    }

    String scaleSet() {
        String urlString = String.format("%s/metadata/instance/compute/vmScaleSetName", endpoint);
        return callGet(urlString, true);
    }

    String accessToken() {
        String urlString = String.format("%s/metadata/identity/oauth2/token", endpoint);
        String accessTokenResponse = callGet(urlString, false);
        return extractAccessToken(accessTokenResponse);
    }

    private String extractAccessToken(String accessTokenResponse) {
        return Json.parse(accessTokenResponse).asObject().get("access_token").asString();
    }

    private String body() {
        return String.format("api-version=%s", API_VERSION);
    }

    private String body(String format) {
        return String.format("api-version=%s&format=%s", API_VERSION, format);
    }

    private String callGet(String urlString, boolean textOutput) {
        return RestClient.create(urlString)
                .withHeader("Metadata", "true")
                .withBody(textOutput ? body("text") : body()).get();
    }

}
