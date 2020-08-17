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

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

public class CloudInfoCollector implements MetricsCollector {
    private static final int TIMEOUT = 1000;
    private static final int RESPONSE_OK = 200;

    private static final String KUBERNETES_TOKEN_PATH = "../var/run/secrets/kubernetes.io/serviceaccount/token";
    private static final String DOCKER_FILE_PATH = "../.dockerenv";
    private String awsEndPoint;
    private String azureEndPoint;
    private String gcpEndPoint;

    public CloudInfoCollector(String awsEndpoint, String azureEndpoint, String gcpEndpoint) {
        awsEndPoint = awsEndpoint;
        azureEndPoint = azureEndpoint;
        gcpEndPoint = gcpEndpoint;
    }

    public void modifyEndPoints(String awsEndpoint, String azureEndpoint, String gcpEndpoint) {
        awsEndPoint = awsEndpoint;
        azureEndPoint = azureEndpoint;
        gcpEndPoint = gcpEndpoint;
    }

    public Map<PhoneHomeMetrics, String> computeMetrics(Node hazelcastNode) {

        Map<PhoneHomeMetrics, String> environmentInfo = new HashMap<>();

        if (check(awsEndPoint)) {
            environmentInfo.put(PhoneHomeMetrics.CLOUD, "A");
        } else if (check(azureEndPoint)) {
            environmentInfo.put(PhoneHomeMetrics.CLOUD, "Z");
        } else if (check(gcpEndPoint)) {
            environmentInfo.put(PhoneHomeMetrics.CLOUD, "G");
        } else {
            environmentInfo.put(PhoneHomeMetrics.CLOUD, "-1");
        }

        File dockerFile = new File(DOCKER_FILE_PATH);
        if (dockerFile.exists() && !dockerFile.isDirectory()) {
            File kubernetesToken = new File(KUBERNETES_TOKEN_PATH);
            if (kubernetesToken.exists() && !kubernetesToken.isDirectory()) {
                environmentInfo.put(PhoneHomeMetrics.DOCKER, "K");
            } else {
                environmentInfo.put(PhoneHomeMetrics.DOCKER, "D");
            }
        } else {
            environmentInfo.put(PhoneHomeMetrics.DOCKER, "N");
        }
        return environmentInfo;
    }

    private boolean check(String urlStr) {
        HttpURLConnection conn = null;
        boolean response;
        try {
            URL url = new URL(urlStr);
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(TIMEOUT * 2);
            conn.setReadTimeout(TIMEOUT * 2);
            conn.connect();
            response = conn.getResponseCode() == RESPONSE_OK;
        } catch (Exception ignored) {
            ignore(ignored);
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
        return response;
    }
}
