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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@SuppressFBWarnings
class CloudInfoCollector implements MetricsCollector {

    private static final String AWS_ENDPOINT = "http://169.254.169.254/latest/meta-data";
    private static final String AZURE_ENDPOINT = " http://169.254.169.254/metadata/instance/compute?api-version=2018-02-01";
    private static final String GCP_ENDPOINT = " http://metadata.google.internal";
    private static final Path KUBERNETES_TOKEN_PATH = Paths.get("/var/run/secrets/kubernetes.io/serviceaccount/token");
    private static final Path DOCKER_FILE_PATH = Paths.get("/.dockerenv");

    private final String awsEndpoint;
    private final String azureEndpoint;
    private final String gcpEndpoint;
    private final Path kubernetesTokenPath;
    private final Path dockerFilePath;

    private volatile Map<PhoneHomeMetrics, String> environmentInfo;

    CloudInfoCollector() {
        this(AWS_ENDPOINT, AZURE_ENDPOINT, GCP_ENDPOINT, KUBERNETES_TOKEN_PATH, DOCKER_FILE_PATH);
    }

    CloudInfoCollector(String awsEndPoint, String azureEndPoint, String gcpEndPoint, Path kubernetesTokenpath,
                       Path dockerFilepath) {
        awsEndpoint = awsEndPoint;
        azureEndpoint = azureEndPoint;
        gcpEndpoint = gcpEndPoint;
        kubernetesTokenPath = kubernetesTokenpath;
        dockerFilePath = dockerFilepath;
    }

    public Map<PhoneHomeMetrics, String> computeMetrics(Node hazelcastNode) {
        if (environmentInfo != null) {
            return environmentInfo;
        }
        Map<PhoneHomeMetrics, String> environmentInfoCollectorMap = new HashMap<>();
        if (MetricsCollector.fetchWebService(awsEndpoint)) {
            environmentInfoCollectorMap.put(PhoneHomeMetrics.CLOUD, "A");
        } else if (MetricsCollector.fetchWebService(azureEndpoint)) {
            environmentInfoCollectorMap.put(PhoneHomeMetrics.CLOUD, "Z");
        } else if (MetricsCollector.fetchWebService(gcpEndpoint)) {
            environmentInfoCollectorMap.put(PhoneHomeMetrics.CLOUD, "G");
        } else {
            environmentInfoCollectorMap.put(PhoneHomeMetrics.CLOUD, "N");
        }
        try {
            dockerFilePath.toRealPath();
            try {
                kubernetesTokenPath.toRealPath();
                environmentInfoCollectorMap.put(PhoneHomeMetrics.DOCKER, "K");
            } catch (IOException e) {
                environmentInfoCollectorMap.put(PhoneHomeMetrics.DOCKER, "D");
            }
        } catch (IOException e) {
            environmentInfoCollectorMap.put(PhoneHomeMetrics.DOCKER, "N");
        }
        environmentInfo = new HashMap<>(environmentInfoCollectorMap);
        return environmentInfo;
    }
}
