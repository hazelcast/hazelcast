/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.util.phonehome.MetricsProvider.fetchWebService;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CLOUD;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.DOCKER;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.VIRIDIAN;

/**
 * Provides information about cloud deployment
 */
@SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
class CloudInfoProvider implements MetricsProvider {
    static final HazelcastProperty AWS_ENDPOINT = new HazelcastProperty(
            "hazelcast.phonehome.endpoint.aws", "http://169.254.169.254/latest/meta-data");
    static final HazelcastProperty AZURE_ENDPOINT = new HazelcastProperty(
            "hazelcast.phonehome.endpoint.azure", "http://169.254.169.254/metadata/instance/compute?api-version=2018-02-01");
    static final HazelcastProperty GCP_ENDPOINT = new HazelcastProperty(
            "hazelcast.phonehome.endpoint.gcp", "http://metadata.google.internal");
    static final HazelcastProperty KUBERNETES_TOKEN_PATH = new HazelcastProperty(
            "hazelcast.phonehome.path.kubernetes.token", "/var/run/secrets/kubernetes.io/serviceaccount/token");
    static final HazelcastProperty DOCKER_FILE_PATH = new HazelcastProperty(
            "hazelcast.phonehome.path.docker.file", "/.dockerenv");

    static final String CLOUD_ENVIRONMENT_ENV_VAR = "HZ_CLOUD_ENVIRONMENT";

    private volatile Map<Metric, String> environmentInfo;

    @Override
    public void provideMetrics(Node node, MetricsCollectionContext context) {
        if (environmentInfo != null) {
            environmentInfo.forEach(context::collect);
            return;
        }

        HazelcastProperties props = node.getProperties();
        Map<Metric, String> info = new HashMap<>(2);

        if (fetchWebService(props.getString(AWS_ENDPOINT))) {
            info.put(CLOUD, "A");
        } else if (fetchWebService(props.getString(AZURE_ENDPOINT))) {
            info.put(CLOUD, "Z");
        } else if (fetchWebService(props.getString(GCP_ENDPOINT))) {
            info.put(CLOUD, "G");
        } else if (fetchWebService(props.getString(AWS_ENDPOINT), RESPONSE_UNAUTHORIZED)) {
            info.put(CLOUD, "A");
        } else {
            info.put(CLOUD, "N");
        }

        try {
            Paths.get(props.getString(DOCKER_FILE_PATH)).toRealPath();
            try {
                Paths.get(props.getString(KUBERNETES_TOKEN_PATH)).toRealPath();
                info.put(DOCKER, "K");
            } catch (IOException e) {
                info.put(DOCKER, "D");
            }
        } catch (IOException e) {
            info.put(DOCKER, "N");
        }

        String cloudEnv = System.getenv(CLOUD_ENVIRONMENT_ENV_VAR);
        if (cloudEnv != null) {
            info.put(VIRIDIAN, cloudEnv);
        }

        environmentInfo = info;
        environmentInfo.forEach(context::collect);
    }
}
