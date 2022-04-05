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

package com.hazelcast.client.impl.spi.impl.discovery;

import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Discovery service that discover nodes via hazelcast.cloud
 * https://coordinator.hazelcast.cloud/cluster/discovery?token=<TOKEN>
 */
public class HazelcastCloudDiscovery {

    /**
     * Internal client property to change base url of cloud discovery endpoint.
     * Used for testing cloud discovery.
     */
    public static final HazelcastProperty CLOUD_URL_BASE_PROPERTY =
            new HazelcastProperty("hazelcast.client.cloud.url", "https://coordinator.hazelcast.cloud");
    private static final String CLOUD_URL_PATH = "/cluster/discovery?token=";
    private static final String PRIVATE_ADDRESS_PROPERTY = "private-address";
    private static final String PUBLIC_ADDRESS_PROPERTY = "public-address";

    private final String endpointUrl;
    private final int connectionTimeoutInMillis;

    public HazelcastCloudDiscovery(String endpointUrl, int connectionTimeoutInMillis) {
        this.endpointUrl = endpointUrl;
        this.connectionTimeoutInMillis = connectionTimeoutInMillis;
    }

    public Map<Address, Address> discoverNodes() {
        try {
            return callService();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private Map<Address, Address> callService() throws IOException, CertificateException {
        URL url = new URL(endpointUrl);
        HttpURLConnection httpsConnection = (HttpURLConnection) url.openConnection();
        httpsConnection.setRequestMethod("GET");
        httpsConnection.setConnectTimeout(connectionTimeoutInMillis);
        httpsConnection.setReadTimeout(connectionTimeoutInMillis);
        httpsConnection.setRequestProperty("Accept-Charset", "UTF-8");
        httpsConnection.connect();
        checkCertificate(httpsConnection);
        checkError(httpsConnection);
        InputStream inputStream = httpsConnection.getInputStream();
        return parseJsonResponse(Json.parse(readInputStream(inputStream)));
    }

    private void checkCertificate(HttpURLConnection connection) throws IOException, CertificateException {
        if (!(connection instanceof HttpsURLConnection)) {
            return;
        }
        HttpsURLConnection con = (HttpsURLConnection) connection;
        for (Certificate cert : con.getServerCertificates()) {
            if (cert instanceof X509Certificate) {
                ((X509Certificate) cert).checkValidity();
            } else {
                throw new CertificateException("Invalid certificate from hazelcast.cloud endpoint");
            }
        }
    }

    static Map<Address, Address> parseJsonResponse(JsonValue jsonValue) throws IOException {
        List<JsonValue> response = jsonValue.asArray().values();

        Map<Address, Address> privateToPublicAddresses = new HashMap<Address, Address>();
        for (JsonValue value : response) {
            String privateAddress = value.asObject().get(PRIVATE_ADDRESS_PROPERTY).asString();
            String publicAddress = value.asObject().get(PUBLIC_ADDRESS_PROPERTY).asString();

            Address publicAddr = createAddress(publicAddress, -1);
            //if it is not explicitly given, create the private address with public addresses port
            Address privateAddr = createAddress(privateAddress, publicAddr.getPort());
            privateToPublicAddresses.put(privateAddr, publicAddr);
        }
        return privateToPublicAddresses;
    }

    private static Address createAddress(String hostname, int defaultPort) throws IOException {
        AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(hostname, defaultPort);
        String scopedHostName = AddressHelper.getScopedHostName(addressHolder);
        return new Address(scopedHostName, addressHolder.getPort());
    }

    private static String readInputStream(InputStream is) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        String line;
        StringBuilder response = new StringBuilder();
        while ((line = in.readLine()) != null) {
            response.append(line);
        }
        in.close();
        return response.toString();
    }

    private static void checkError(HttpURLConnection httpConnection) throws IOException {
        int responseCode = httpConnection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            String errorMessage = extractErrorMessage(httpConnection);
            throw new IOException(errorMessage);
        }
    }

    private static String extractErrorMessage(HttpURLConnection httpConnection) {
        InputStream errorStream = httpConnection.getErrorStream();
        return errorStream == null ? "" : readFrom(errorStream);
    }

    private static String readFrom(InputStream stream) {
        Scanner scanner = (new Scanner(stream, "UTF-8")).useDelimiter("\\A");
        return scanner.hasNext() ? scanner.next() : "";
    }

    public static String createUrlEndpoint(String cloudBaseUrl, String cloudToken) {
        return cloudBaseUrl + CLOUD_URL_PATH + cloudToken;
    }

}
