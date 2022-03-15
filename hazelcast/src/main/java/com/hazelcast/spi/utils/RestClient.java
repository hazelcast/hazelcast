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

package com.hazelcast.spi.utils;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.spi.exception.RestClientException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class RestClient {

    /**
     * HTTP status code 200 OK
     */
    public static final int HTTP_OK = 200;

    /**
     * HTTP status code 404 NOT FOUND
     */
    public static final int HTTP_NOT_FOUND = 404;

    private final String url;
    private final List<Parameter> headers = new ArrayList<>();
    private Set<Integer> expectedResponseCodes;
    private String body;
    private int readTimeoutSeconds;
    private int connectTimeoutSeconds;
    private int retries;
    private String caCertificate;

    protected RestClient(String url) {
        this.url = url;
    }

    public static RestClient create(String url) {
        return new RestClient(url);
    }

    public RestClient withHeaders(Map<String, String> headers) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            withHeader(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public RestClient withHeader(String name, String value) {
        this.headers.add(new Parameter(name, value));
        return this;
    }

    public RestClient withBody(String body) {
        this.body = body;
        return this;
    }

    public RestClient withReadTimeoutSeconds(int readTimeoutSeconds) {
        this.readTimeoutSeconds = readTimeoutSeconds;
        return this;
    }

    public RestClient withConnectTimeoutSeconds(int connectTimeoutSeconds) {
        this.connectTimeoutSeconds = connectTimeoutSeconds;
        return this;
    }

    public RestClient withRetries(int retries) {
        this.retries = retries;
        return this;
    }

    public RestClient withCaCertificates(String caCertificate) {
        this.caCertificate = caCertificate;
        return this;
    }

    public RestClient expectResponseCodes(Integer... codes) {
        if (expectedResponseCodes == null) {
            expectedResponseCodes = new HashSet<>();
        }
        expectedResponseCodes.addAll(Arrays.asList(codes));
        return this;
    }

    public Response get() {
        return callWithRetries("GET");
    }

    public Response post() {
        return callWithRetries("POST");
    }

    private Response callWithRetries(String method) {
        return RetryUtils.retry(() -> call(method), retries);
    }

    private Response call(String method) {
        HttpURLConnection connection = null;
        try {
            URL urlToConnect = new URL(url);
            connection = (HttpURLConnection) urlToConnect.openConnection();
            if (connection instanceof HttpsURLConnection && caCertificate != null) {
                ((HttpsURLConnection) connection).setSSLSocketFactory(buildSslSocketFactory());
            }
            connection.setReadTimeout((int) TimeUnit.SECONDS.toMillis(readTimeoutSeconds));
            connection.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(connectTimeoutSeconds));
            connection.setRequestMethod(method);
            for (Parameter header : headers) {
                connection.setRequestProperty(header.getKey(), header.getValue());
            }
            if (body != null) {
                byte[] bodyData = body.getBytes(StandardCharsets.UTF_8);

                connection.setDoOutput(true);
                connection.setRequestProperty("charset", "utf-8");
                connection.setRequestProperty("Content-Length", Integer.toString(bodyData.length));

                try (DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream())) {
                    outputStream.write(bodyData);
                    outputStream.flush();
                }
            }

            checkResponseCode(method, connection);
            return new Response(connection.getResponseCode(), read(connection));
        } catch (IOException e) {
            throw new RestClientException("Failure in executing REST call", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private void checkResponseCode(String method, HttpURLConnection connection)
            throws IOException {
        int responseCode = connection.getResponseCode();
        if (!isExpectedResponseCode(responseCode)) {
            String errorMessage;
            try {
                errorMessage = read(connection);
            } catch (Exception e) {
                throw new RestClientException(
                        String.format("Failure executing: %s at: %s", method, url), responseCode);
            }
            throw new RestClientException(String.format("Failure executing: %s at: %s. Message: %s", method, url, errorMessage),
                    responseCode);
        }
    }

    private boolean isExpectedResponseCode(int responseCode) {
        // expect HTTP_OK by default
        return expectedResponseCodes == null
                ? responseCode == HTTP_OK
                : expectedResponseCodes.contains(responseCode);
    }

    /**
     * Builds SSL Socket Factory with the public CA Certificate from Kubernetes Master.
     */
    private SSLSocketFactory buildSslSocketFactory() {
        try {
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);

            int i = 0;
            for (Certificate certificate : generateCertificates()) {
                String alias = String.format("ca-%d", i++);
                keyStore.setCertificateEntry(alias, certificate);
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);

            SSLContext context = SSLContext.getInstance("TLSv1.2");
            context.init(null, tmf.getTrustManagers(), null);
            return context.getSocketFactory();

        } catch (Exception e) {
            throw new RestClientException("Failure in generating SSLSocketFactory", e);
        }
    }

    /**
     * Generates CA Certificate from the default CA Cert file or from the externally provided "ca-certificate" property.
     */
    private Collection<? extends Certificate> generateCertificates()
            throws CertificateException {
        InputStream caInput = null;
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            caInput = new ByteArrayInputStream(caCertificate.getBytes(StandardCharsets.UTF_8));
            return cf.generateCertificates(caInput);
        } finally {
            IOUtil.closeResource(caInput);
        }
    }

    private static String read(HttpURLConnection connection) {
        InputStream stream;
        try {
            stream = connection.getInputStream();
        } catch (IOException e) {
            stream = connection.getErrorStream();
        }
        if (stream == null) {
            return null;
        }
        Scanner scanner = new Scanner(stream, "UTF-8");
        scanner.useDelimiter("\\Z");
        return scanner.next();
    }

    public static class Response {

        private final int code;
        private final String body;

        Response(int code, String body) {
            this.code = code;
            this.body = body;
        }

        public int getCode() {
            return code;
        }

        public String getBody() {
            return body;
        }
    }

    private static final class Parameter {
        private final String key;
        private final String value;

        private Parameter(String key, String value) {
            this.key = key;
            this.value = value;
        }

        private String getKey() {
            return key;
        }

        private String getValue() {
            return value;
        }
    }
}
