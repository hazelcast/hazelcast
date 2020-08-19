/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class CloudInfoCollector implements MetricsCollector {
    private final Path KUBERNETES_TOKEN_PATH = Paths.get("/var/run/secrets/kubernetes.io/serviceaccount/token");
    private final Path DOCKER_FILE_PATH = Paths.get("/.dockerenv");

    private final String awsEndPoint;
    private final String azureEndPoint;
    private final String gcpEndPoint;

    public CloudInfoCollector(String awsEndpoint, String azureEndpoint, String gcpEndpoint) {
        awsEndPoint = awsEndpoint;
        azureEndPoint = azureEndpoint;
        gcpEndPoint = gcpEndpoint;
    }

    public Map<PhoneHomeMetrics, String> computeMetrics(Node hazelcastNode) {

        Map<PhoneHomeMetrics, String> environmentInfo = new HashMap<>();

        if (MetricsCollector.fetchWebService(awsEndPoint)) {
            environmentInfo.put(PhoneHomeMetrics.CLOUD, "A");
        } else if (MetricsCollector.fetchWebService(azureEndPoint)) {
            environmentInfo.put(PhoneHomeMetrics.CLOUD, "Z");
        } else if (MetricsCollector.fetchWebService(gcpEndPoint)) {
            environmentInfo.put(PhoneHomeMetrics.CLOUD, "G");
        } else {
            environmentInfo.put(PhoneHomeMetrics.CLOUD, "-1");
        }
        try {
            DOCKER_FILE_PATH.toRealPath();
            try {
                KUBERNETES_TOKEN_PATH.toRealPath();
                environmentInfo.put(PhoneHomeMetrics.DOCKER, "K");
            } catch (IOException e) {
                environmentInfo.put(PhoneHomeMetrics.DOCKER, "D");
            }
        } catch (IOException e) {
            e.printStackTrace();
            environmentInfo.put(PhoneHomeMetrics.DOCKER, "N");
        }
        return environmentInfo;
    }
}
