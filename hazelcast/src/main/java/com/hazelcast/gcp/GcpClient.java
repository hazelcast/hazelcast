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

package com.hazelcast.gcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.exception.RestClientException;
import com.hazelcast.spi.utils.RetryUtils;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Responsible for fetching the discovery information from GCP APIs.
 */
class GcpClient {
    private static final ILogger LOGGER = Logger.getLogger(GcpDiscoveryStrategy.class);

    private static final int HTTP_UNAUTHORIZED = 401;
    private static final int HTTP_FORBIDDEN = 403;
    private static final int HTTP_NOT_FOUND = 404;

    private static final int RETRIES = 3;
    private static final List<String> NON_RETRYABLE_KEYWORDS = asList("Private key json file not found",
            "Request had insufficient authentication scopes", "Required 'compute.instances.list' permission",
            "Service account not enabled on this instance");

    private boolean isKnownExceptionAlreadyLogged;

    private final GcpMetadataApi gcpMetadataApi;
    private final GcpComputeApi gcpComputeApi;
    private final GcpAuthenticator gcpAuthenticator;

    private final String privateKeyPath;
    private final List<String> projects;
    private final List<String> zones;
    private final Label label;

    GcpClient(GcpMetadataApi gcpMetadataApi, GcpComputeApi gcpComputeApi, GcpAuthenticator gcpAuthenticator,
              GcpConfig gcpConfig) {
        this.gcpMetadataApi = gcpMetadataApi;
        this.gcpComputeApi = gcpComputeApi;
        this.gcpAuthenticator = gcpAuthenticator;

        this.privateKeyPath = gcpConfig.getPrivateKeyPath();
        this.projects = projectFromConfigOrMetadataApi(gcpConfig);
        this.zones = zonesFromConfigOrComputeApi(gcpConfig);
        this.label = gcpConfig.getLabel();
    }

    private List<String> projectFromConfigOrMetadataApi(final GcpConfig gcpConfig) {
        if (!gcpConfig.getProjects().isEmpty()) {
            return gcpConfig.getProjects();
        }
        LOGGER.finest("Property 'projects' not configured, fetching the current GCP project");
        return singletonList(RetryUtils.retry(gcpMetadataApi::currentProject, RETRIES, NON_RETRYABLE_KEYWORDS));
    }

    private List<String> zonesFromConfigOrComputeApi(final GcpConfig gcpConfig) {
        try {
            if (gcpConfig.getRegion() != null) {
                LOGGER.finest("Property 'region' configured, fetching GCP zones of the specified GCP region");
                return RetryUtils.retry(() -> fetchZones(gcpConfig.getRegion()), RETRIES, NON_RETRYABLE_KEYWORDS);
            }

            if (!gcpConfig.getZones().isEmpty()) {
                return gcpConfig.getZones();
            }

            LOGGER.finest("Property 'zones' not configured, fetching GCP zones of the current GCP region");
            return RetryUtils.retry(() -> {
                String region = gcpMetadataApi.currentRegion();
                return fetchZones(region);
            }, RETRIES, NON_RETRYABLE_KEYWORDS);
        } catch (RestClientException e) {
            handleKnownException(e);
            return emptyList();
        }
    }

    List<GcpAddress> getAddresses() {
        try {
            return RetryUtils.retry(this::fetchGcpAddresses, RETRIES, NON_RETRYABLE_KEYWORDS);
        } catch (RestClientException e) {
            handleKnownException(e);
            return emptyList();
        }
    }

    private List<String> fetchZones(String region) {
        List<String> zones = new ArrayList<>();
        String accessToken = fetchAccessToken();
        for (String project : projects) {
            zones.addAll(gcpComputeApi.zones(project, region, accessToken));
        }
        return zones;
    }

    private List<GcpAddress> fetchGcpAddresses() {
        LOGGER.finest("Fetching OAuth Access Token");
        final String accessToken = fetchAccessToken();

        List<GcpAddress> result = new ArrayList<>();
        for (final String project : projects) {
            for (final String zone : zones) {
                LOGGER.finest(String.format("Fetching instances for project '%s' and zone '%s'", project, zone));
                List<GcpAddress> addresses = gcpComputeApi.instances(project, zone, label, accessToken);
                LOGGER.finest(String.format("Found the following instances for project '%s' and zone '%s': %s", project, zone,
                        addresses));
                result.addAll(addresses);
            }
        }
        return result;
    }

    private String fetchAccessToken() {
        if (privateKeyPath != null) {
            return gcpAuthenticator.refreshAccessToken(privateKeyPath);
        }
        return gcpMetadataApi.accessToken();
    }

    String getAvailabilityZone() {
        return gcpMetadataApi.currentZone();
    }

    private void handleKnownException(RestClientException e) {
        if (e.getHttpErrorCode() == HTTP_UNAUTHORIZED) {
            if (!isKnownExceptionAlreadyLogged) {
                LOGGER.warning("Google Cloud API Authorization failed! Check your credentials. Starting standalone.");
                isKnownExceptionAlreadyLogged = true;
            }
        } else if (e.getHttpErrorCode() == HTTP_FORBIDDEN) {
            if (!isKnownExceptionAlreadyLogged) {
                LOGGER.warning("Google Cloud API access is forbidden! Starting standalone. To use Hazelcast GCP discovery, "
                        + "make sure that your service account has at minimum \"Read Only\" Access Scope to Compute Engine API.");
                isKnownExceptionAlreadyLogged = true;
            }
        } else if (e.getHttpErrorCode() == HTTP_NOT_FOUND) {
            if (!isKnownExceptionAlreadyLogged) {
                LOGGER.warning("Google Cloud API Not Found! Starting standalone. Please check that you have a service account "
                        + "assigned to your VM instance or `private-key-path` property defined.");
                isKnownExceptionAlreadyLogged = true;
            }
        } else {
            throw e;
        }
        LOGGER.finest(e);
    }
}
