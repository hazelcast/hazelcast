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
import com.hazelcast.spi.exception.NoCredentialsException;
import com.hazelcast.spi.utils.RestClient;

/**
 * Fetches OAuth 2.0 Access Token from Microsoft Azure API.
 *
 * @see <a href="https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-manager-api-authentication">
 * Using OAuth 2.0</a>
 */
class AzureAuthenticator {
    private static final String AZURE_AUTH_ENDPOINT = "https://login.microsoftonline.com";
    private static final String RESOURCE = "https://management.azure.com";
    private static final String GRANT_TYPE = "client_credentials";

    private final String endpoint;

    AzureAuthenticator() {
        this.endpoint = AZURE_AUTH_ENDPOINT;
    }

    /**
     * For test purposes only.
     */
    AzureAuthenticator(String endpoint) {
        this.endpoint = endpoint;
    }

    String refreshAccessToken(String tenantId, String clientId, String clientSecret) {
        try {
            String accessTokenJson = callService(urlFor(tenantId), body(clientId, clientSecret));
            return extractAccessToken(accessTokenJson);
        } catch (Exception e) {
            throw new NoCredentialsException("Error while fetching access token from Azure API using client-secret", e);
        }
    }

    private String body(String clientId, String clientSecret) {
        return String.format("grant_type=%s&resource=%s&client_id=%s&client_secret=%s", GRANT_TYPE, RESOURCE,
                clientId, clientSecret);
    }

    private String urlFor(String tenantId) {
        return String.format("%s/%s/oauth2/token", endpoint, tenantId);
    }

    private String callService(String url, String body) {
        return RestClient.create(url).withBody(body).get().getBody();
    }

    private String extractAccessToken(String accessTokenJson) {
        return Json.parse(accessTokenJson).asObject().get("access_token").asString();
    }
}
