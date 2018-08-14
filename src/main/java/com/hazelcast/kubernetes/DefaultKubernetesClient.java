/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.nio.IOUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

/**
 * Responsible for connecting to the Kubernetes API.
 * <p>
 * Note: This client should always be used from inside Kubernetes since it depends on the CA Cert file, which exists
 * in the POD filesystem.
 *
 * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/">Kubernetes API</a>
 */
class DefaultKubernetesClient
        implements KubernetesClient {
    private static final int HTTP_OK = 200;

    private final String kubernetesMaster;
    private final String apiToken;

    DefaultKubernetesClient(String kubernetesMaster, String apiToken) {
        this.kubernetesMaster = kubernetesMaster;
        this.apiToken = apiToken;
    }

    @Override
    public Endpoints endpoints(String namespace) {
        String urlString = String.format("%s/api/v1/namespaces/%s/endpoints", kubernetesMaster, namespace);
        return parseEndpointsList(callGet(urlString));

    }

    @Override
    public Endpoints endpointsByLabel(String namespace, String serviceLabel, String serviceLabelValue) {
        String param = String.format("labelSelector=%s=%s", serviceLabel, serviceLabelValue);
        String urlString = String.format("%s/api/v1/namespaces/%s/endpoints?%s", kubernetesMaster, namespace, param);
        return parseEndpointsList(callGet(urlString));
    }

    @Override
    public Endpoints endpointsByName(String namespace, String endpointName) {
        String urlString = String.format("%s/api/v1/namespaces/%s/endpoints/%s", kubernetesMaster, namespace, endpointName);
        JsonObject json = callGet(urlString);
        return parseEndpoint(json);
    }

    private JsonObject callGet(String urlString) {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(urlString);
            connection = (HttpURLConnection) url.openConnection();
            if (connection instanceof HttpsURLConnection) {
                ((HttpsURLConnection) connection).setSSLSocketFactory(buildSslSocketFactory());
            }
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Authorization", String.format("Bearer %s", apiToken));

            if (connection.getResponseCode() != HTTP_OK) {
                throw new KubernetesClientException(String.format("Failure executing: GET at: %s. Message: %s,", urlString,
                        read(connection.getErrorStream())));
            }
            return Json.parse(read(connection.getInputStream())).asObject();
        } catch (Exception e) {
            throw new KubernetesClientException("Failure in KubernetesClient", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static String read(InputStream stream) {
        Scanner scanner = new Scanner(stream, "UTF-8");
        scanner.useDelimiter("\\Z");
        return scanner.next();
    }

    private static Endpoints parseEndpointsList(JsonObject json) {
        List<EntrypointAddress> addresses = new ArrayList<EntrypointAddress>();
        List<EntrypointAddress> notReadyAddresses = new ArrayList<EntrypointAddress>();

        for (JsonValue object : toJsonArray(json.get("items"))) {
            Endpoints endpoints = parseEndpoint(object);
            addresses.addAll(endpoints.getAddresses());
            notReadyAddresses.addAll(endpoints.getNotReadyAddresses());
        }

        return new Endpoints(addresses, notReadyAddresses);
    }

    private static Endpoints parseEndpoint(JsonValue endpointsJson) {
        List<EntrypointAddress> addresses = new ArrayList<EntrypointAddress>();
        List<EntrypointAddress> notReadyAddresses = new ArrayList<EntrypointAddress>();

        for (JsonValue subset : toJsonArray(endpointsJson.asObject().get("subsets"))) {
            for (JsonValue address : toJsonArray(subset.asObject().get("addresses"))) {
                addresses.add(parseEntrypointAddress(address));
            }
            for (JsonValue notReadyAddress : toJsonArray(subset.asObject().get("notReadyAddresses"))) {
                notReadyAddresses.add(parseEntrypointAddress(notReadyAddress));
            }
        }
        return new Endpoints(addresses, notReadyAddresses);
    }

    private static EntrypointAddress parseEntrypointAddress(JsonValue endpointAddressJson) {
        String ip = endpointAddressJson.asObject().get("ip").asString();
        Map<String, Object> additionalProperties = parseAdditionalProperties(endpointAddressJson);
        return new EntrypointAddress(ip, additionalProperties);
    }

    private static Map<String, Object> parseAdditionalProperties(JsonValue endpointAddressJson) {
        Set<String> knownFieldNames = new HashSet<String>(Arrays.asList("ip", "nodeName", "targetRef", "hostname"));

        Map<String, Object> result = new HashMap<String, Object>();
        Iterator<JsonObject.Member> iter = endpointAddressJson.asObject().iterator();
        while (iter.hasNext()) {
            JsonObject.Member member = iter.next();
            if (!knownFieldNames.contains(member.getName())) {
                result.put(member.getName(), toString(member.getValue()));
            }
        }
        return result;
    }

    private static JsonArray toJsonArray(JsonValue jsonValue) {
        if (jsonValue == null || jsonValue.isNull()) {
            return new JsonArray();
        } else {
            return jsonValue.asArray();
        }
    }

    private static String toString(JsonValue jsonValue) {
        if (jsonValue.isString()) {
            return jsonValue.asString();
        } else {
            return jsonValue.toString();
        }
    }

    /**
     * Builds SSL Socket Factory with the public CA Certificate from Kubernetes Master.
     */
    private static SSLSocketFactory buildSslSocketFactory() {
        try {
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);
            keyStore.setCertificateEntry("ca", generateCertificate());

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);

            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, tmf.getTrustManagers(), null);
            return context.getSocketFactory();

        } catch (Exception e) {
            throw new KubernetesClientException("Failure in generating SSLSocketFactory", e);
        }
    }

    /**
     * Generates CA Certificate from the default CA Cert file (which always exists in the Kubernetes POD).
     */
    private static Certificate generateCertificate()
            throws IOException, CertificateException {
        InputStream caInput = null;
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            caInput = new BufferedInputStream(new FileInputStream(new File(caCertPath())));
            return cf.generateCertificate(caInput);
        } finally {
            IOUtil.closeResource(caInput);
        }
    }

    @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
    private static String caCertPath() {
        return "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
    }

}
