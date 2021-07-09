/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.gcp;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonValue;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.gcp.Utils.lastPartOf;

/**
 * Responsible for connecting to the Google Cloud Compute API.
 *
 * @see <a href="https://cloud.google.com/compute/docs/reference/rest/v1/">Compute Engine API</a>
 */
class GcpComputeApi {
    private static final String GOOGLE_API_ENDPOINT = "https://www.googleapis.com";

    private final String endpoint;

    GcpComputeApi() {
        this.endpoint = GOOGLE_API_ENDPOINT;
    }

    /**
     * For test purposes only.
     */
    GcpComputeApi(String endpoint) {
        this.endpoint = endpoint;
    }

    List<GcpAddress> instances(String project, String zone, Label label, String accessToken) {
        String response = RestClient
                .create(urlFor(project, zone, label))
                .withHeader("Authorization", String.format("OAuth %s", accessToken))
                .get();

        List<GcpAddress> result = new ArrayList<GcpAddress>();
        for (JsonValue item : toJsonArray(Json.parse(response).asObject().get("items"))) {
            if ("RUNNING".equals(item.asObject().get("status").asString())) {
                String privateAddress = null;
                String publicAddress = null;
                for (JsonValue networkInterface : toJsonArray(item.asObject().get("networkInterfaces"))) {
                    privateAddress = networkInterface.asObject().getString("networkIP", null);
                    for (JsonValue accessConfig : toJsonArray(networkInterface.asObject().get("accessConfigs"))) {
                        publicAddress = accessConfig.asObject().getString("natIP", null);
                    }
                }
                if (privateAddress != null) {
                    result.add(new GcpAddress(privateAddress, publicAddress));
                }
            }
        }

        return result;
    }

    List<String> zones(String project, String region, String accessToken) {
        String url = String.format("%s/compute/v1/projects/%s/regions/%s?alt=json&fields=zones", endpoint, project, region);
        String response = RestClient
                .create(url)
                .withHeader("Authorization", String.format("OAuth %s", accessToken))
                .get();

        JsonArray zoneUrls = toJsonArray(Json.parse(response).asObject().get("zones"));

        List<String> zones = new ArrayList<>();
        for (JsonValue value : zoneUrls) {
            zones.add(lastPartOf(value.asString(), "/"));
        }
        return zones;
    }

    private String urlFor(String project, String zone, Label label) {
        String url = String.format("%s/compute/v1/projects/%s/zones/%s/instances", endpoint, project, zone);
        if (label != null) {
            url = String.format("%s?filter=labels.%s+eq+%s", url, label.getKey(), label.getValue());
        }
        return url;
    }

    private static JsonArray toJsonArray(JsonValue jsonValue) {
        if (jsonValue == null || jsonValue.isNull()) {
            return new JsonArray();
        } else {
            return jsonValue.asArray();
        }
    }
}
