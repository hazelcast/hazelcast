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

package com.hazelcast.client.spi.impl.discovery;

import com.hazelcast.com.eclipsesource.json.Json;
import com.hazelcast.com.eclipsesource.json.JsonValue;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.nio.Address;
import com.hazelcast.util.AddressUtil;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Discovery service that discover nodes via hazelcast.cloud
 * https://coordinator.hazelcast.cloud/cluster/discovery?token=<TOKEN>
 */
class HazelcastCloudDiscovery {

    private static final String CLOUD_URL = "https://coordinator.hazelcast.cloud/cluster/discovery?token=";
    private static final String PRIVATE_ADDRESS_PROPERTY = "private-address";
    private static final String PUBLIC_ADDRESS_PROPERTY = "public-address";

    private final String endpointUrl;
    private final int connectionTimeoutInMillis;

    HazelcastCloudDiscovery(String cloudToken, int connectionTimeoutInMillis) {
        endpointUrl = CLOUD_URL + cloudToken;
        this.connectionTimeoutInMillis = connectionTimeoutInMillis;
    }

    Map<Address, Address> discoverNodes() {
        try {
            return callService();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private Map<Address, Address> callService() throws IOException, CertificateException {
        URL url = new URL(endpointUrl);
        HttpsURLConnection httpsConnection = (HttpsURLConnection) url.openConnection();
        httpsConnection.setRequestMethod("GET");
        httpsConnection.setConnectTimeout(connectionTimeoutInMillis);
        httpsConnection.setReadTimeout(connectionTimeoutInMillis);
        httpsConnection.setRequestProperty("Accept-Charset", "UTF-8");
        httpsConnection.connect();
        checkCertificate(httpsConnection);
        checkError(httpsConnection);
        return parseResponse(httpsConnection.getInputStream());
    }

    private void checkCertificate(HttpsURLConnection con) throws IOException, CertificateException {
        for (Certificate cert : con.getServerCertificates()) {
            if (cert instanceof X509Certificate) {
                ((X509Certificate) cert).checkValidity();
            } else {
                throw new CertificateException("Invalid certificate from hazelcast.cloud endpoint");
            }
        }
    }

    private Map<Address, Address> parseResponse(InputStream is) throws IOException {

        JsonValue jsonValue = Json.parse(readInputStream(is));
        List<JsonValue> response = jsonValue.asArray().values();

        Map<Address, Address> privateToPublicAddresses = new HashMap<Address, Address>();
        for (JsonValue value : response) {
            String privateAddress = value.asObject().get(PRIVATE_ADDRESS_PROPERTY).asString();
            String publicAddress = value.asObject().get(PUBLIC_ADDRESS_PROPERTY).asString();

            Address publicAddr = createAddress(publicAddress);
            privateToPublicAddresses.put(new Address(privateAddress, publicAddr.getPort()), publicAddr);
        }

        return privateToPublicAddresses;
    }

    private Address createAddress(String hostname) throws IOException {
        AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(hostname);
        String scopedHostName = AddressHelper.getScopedHostName(addressHolder);
        return new Address(scopedHostName, addressHolder.getPort());
    }

    private static String readInputStream(InputStream is) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(is, "UTF-8"));
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

}
