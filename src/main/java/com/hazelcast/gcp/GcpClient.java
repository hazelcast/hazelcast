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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static java.util.Collections.singletonList;

class GcpClient {
    private static final ILogger LOGGER = Logger.getLogger(GcpDiscoveryStrategy.class);

    private static final int RETRIES = 10;

    private final GcpMetadataApi gcpMetadataApi;
    private final GcpComputeApi gcpComputeApi;

    private final List<String> projects;
    private final List<String> zones;
    private final String label;

    GcpClient(GcpMetadataApi gcpMetadataApi, GcpComputeApi gcpComputeApi, GcpConfig gcpConfig) {
        this.gcpMetadataApi = gcpMetadataApi;
        this.gcpComputeApi = gcpComputeApi;

        projects = projectFromConfigOrMetadataApi(gcpConfig);
        zones = zonesFromConfigOrMetadataApi(gcpConfig);
        label = gcpConfig.getLabel();
    }

    private List<String> projectFromConfigOrMetadataApi(final GcpConfig gcpConfig) {
        if (!gcpConfig.getProjects().isEmpty()) {
            return gcpConfig.getProjects();
        }
        LOGGER.finest("Property 'projects' not configured, fetching the current GCP project");
        return singletonList(RetryUtils.retry(new Callable<String>() {
            @Override
            public String call() {
                return gcpMetadataApi.currentProject();
            }
        }, RETRIES));
    }

    private List<String> zonesFromConfigOrMetadataApi(final GcpConfig gcpConfig) {
        if (!gcpConfig.getZones().isEmpty()) {
            return gcpConfig.getZones();
        }
        LOGGER.finest("Property 'zones' not configured, fetching the current GCP zone");
        return singletonList(RetryUtils.retry(new Callable<String>() {
            @Override
            public String call() {
                return gcpMetadataApi.currentZone();
            }
        }, RETRIES));
    }

    List<GcpAddress> getAddresses() {
        LOGGER.finest("Fetching OAuth Access Token");
        final String accessToken = RetryUtils.retry(new Callable<String>() {
            @Override
            public String call() {
                return gcpMetadataApi.accessToken();
            }
        }, RETRIES);

        List<GcpAddress> result = new ArrayList<GcpAddress>();
        for (final String project : projects) {
            for (final String zone : zones) {
                LOGGER.finest(String.format("Fetching instances for project '%s' and zone '%s'", project, zone));
                List<GcpAddress> addresses = RetryUtils.retry(new Callable<List<GcpAddress>>() {
                    @Override
                    public List<GcpAddress> call() {
                        return gcpComputeApi.instances(project, zone, label, accessToken);
                    }
                }, RETRIES);
                LOGGER.finest(String.format("Found the following instances for project '%s' and zone '%s': %s", project, zone,
                        addresses));
                result.addAll(addresses);
            }
        }
        return result;
    }

    String getAvailabilityZone() {
        return gcpMetadataApi.currentZone();
    }
}
