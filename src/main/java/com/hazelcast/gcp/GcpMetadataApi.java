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

package com.hazelcast.gcp;

import com.hazelcast.internal.json.Json;

/**
 * Responsible for connecting to the Google Cloud Instance Metadata API.
 *
 * @see <a href="https://cloud.google.com/appengine/docs/standard/java/accessing-instance-metadata">GCP Instance Metatadata</a>
 */
class GcpMetadataApi {
    private static final String METADATA_ENDPOINT = "http://metadata.google.internal";

    private final String endpoint;

    GcpMetadataApi() {
        this.endpoint = METADATA_ENDPOINT;
    }

    /**
     * For test purposes only.
     */
    GcpMetadataApi(String endpoint) {
        this.endpoint = endpoint;
    }

    String currentProject() {
        String urlString = String.format("%s/computeMetadata/v1/project/project-id", endpoint);
        return callGet(urlString);
    }

    String currentZone() {
        String urlString = String.format("%s/computeMetadata/v1/instance/zone", endpoint);
        String zoneResponse = callGet(urlString);
        return lastPartOf(zoneResponse);
    }

    private static String lastPartOf(String string) {
        String[] parts = string.split("/");
        return parts[parts.length - 1];
    }

    String accessToken() {
        String urlString = String.format("%s/computeMetadata/v1/instance/service-accounts/default/token", endpoint);
        String accessTokenResponse = callGet(urlString);
        return extractAccessToken(accessTokenResponse);
    }

    private static String extractAccessToken(String accessTokenResponse) {
        return Json.parse(accessTokenResponse).asObject().get("access_token").asString();
    }

    private static String callGet(String urlString) {
        return RestClient.create(urlString).withHeader("Metadata-Flavor", "Google").get();
    }
}
