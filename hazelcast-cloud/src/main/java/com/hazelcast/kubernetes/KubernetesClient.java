/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.kubernetes;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.nio.IOUtil;

/**
 * A Kubernetes client that can retrieve a list of pod IP addresses
 * from the Kubernetes API server, and optionally, filter by labels.
 */
public class KubernetesClient {
    private final KubernetesConfig kubernetesConfig;
    private final String baseUrl;

    public KubernetesClient(KubernetesConfig kubernetesConfig) {
        if (kubernetesConfig == null) {
            throw new IllegalArgumentException("KubernetesConfig is required!");
        }
        if (kubernetesConfig.getHost() == null) {
            throw new IllegalArgumentException("Kubernetes host is required!");
        }
        if (kubernetesConfig.getPort() == null) {
            throw new IllegalArgumentException("Kubernetes port is required!");
        }
        if (kubernetesConfig.getVersion() == null) {
            throw new IllegalArgumentException("Kubernetes version is required!");
        }

        this.kubernetesConfig = kubernetesConfig;
        baseUrl = String.format("http://%s:%s/api/%s",
                kubernetesConfig.getHost(), kubernetesConfig.getPort(), kubernetesConfig.getVersion());
    }

    public Collection<String> getPodIpAddresses() throws Exception {
        JsonObject pods = getPods(kubernetesConfig.getLabelQuery());
        LinkedList<String> addresses = new LinkedList<String>();

        JsonValue items = pods.get("items");
        if (items == null || items.isNull() || !items.isArray()) {
            return addresses;
        }
        for (JsonValue item : items.asArray()) {
            String ip = extractPodIpFromItem(item);
            if (ip != null) {
                addresses.push(ip);
            }
        }

        return Collections.unmodifiableCollection(addresses);
    }

    private String extractPodIpFromItem(JsonValue item) {
        if (item.isNull() || !item.isObject()) {
            return null;
        }

        JsonValue currentState = item.asObject().get("currentState");
        if (currentState == null || currentState.isNull() || !currentState.isObject()) {
            return null;
        }

        JsonValue ip = currentState.asObject().get("podIP");
        if (ip == null || ip.isNull() || !ip.isString()) {
            return null;
        }

        return ip.asString();
    }

    protected JsonObject getPods(String labelQuery) throws IOException {
        String url = baseUrl + "/pods";
        if (labelQuery != null && labelQuery.length() > 0) {
            url += "?labels=" + URLEncoder.encode(labelQuery, "UTF-8");
        }
        InputStream is = null;
        try {
            is = openStream(url);
            return JsonObject.readFrom(new InputStreamReader(is));
        } finally {
            IOUtil.closeResource(is);
        }
    }

    private static InputStream openStream(String url) throws IOException {
        URL httpUrl = new URL(url);
        HttpURLConnection httpConnection = (HttpURLConnection) (httpUrl.openConnection());
        httpConnection.setRequestMethod("GET");
        httpConnection.setDoOutput(false);
        httpConnection.connect();
        return httpConnection.getInputStream();
    }
}
