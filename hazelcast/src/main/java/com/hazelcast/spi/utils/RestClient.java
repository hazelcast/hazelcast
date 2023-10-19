/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public final class RestClient {

    /**
     * HTTP status code 200 OK
     */
    public static final int HTTP_OK = 200;

    /**
     * HTTP status code 404 NOT FOUND
     */
    public static final int HTTP_NOT_FOUND = 404;
    /**
     * Default value of -1 results in connection timeout not being explicitly defined
     */
    public static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = -1;

    private static final String WATCH_FORMAT = "watch=1&resourceVersion=%s";

    private final String url;
    private final List<Parameter> headers = new ArrayList<>();
    private final HttpClient httpClient;
    private Set<Integer> expectedResponseCodes;
    private String body;
    private int requestTimeoutSeconds;
    private int retries;

    /**
     * Build a new RestClient, backed by an {@link HttpClient} using {@link SSLContext}
     * if a {@code non-null} caCertificate is provided. Initial connection attempt will
     * be made with the provided connection timeout if greater than zero.
     *
     * @param url                   the URL to connect to
     * @param caCertificate         the SSL certificate to use, or null
     * @param connectTimeoutSeconds the timeout used during initial connection attempt, or
     *                              0 if no connection timeout should be defined
     */
    private RestClient(String url, @Nullable String caCertificate, int connectTimeoutSeconds) {
        this.url = url;
        HttpClient.Builder builder = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1);
        if (connectTimeoutSeconds > 0) {
            builder.connectTimeout(Duration.ofSeconds(connectTimeoutSeconds));
        }
        if (caCertificate != null) {
            builder.sslContext(buildSslContext(caCertificate));
        }
        this.httpClient = builder.build();
    }

    public static RestClient create(String url) {
        return create(url, DEFAULT_CONNECT_TIMEOUT_SECONDS);
    }

    public static RestClient create(String url, int connectTimeoutSeconds) {
        return new RestClient(url, null, connectTimeoutSeconds);
    }

    public static RestClient createWithSSL(String url, String caCertificate) {
        return createWithSSL(url, caCertificate, DEFAULT_CONNECT_TIMEOUT_SECONDS);
    }

    public static RestClient createWithSSL(String url, String caCertificate, int connectTimeoutSeconds) {
        return new RestClient(url, caCertificate, connectTimeoutSeconds);
    }

    public RestClient withHeaders(Map<String, String> headers) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            withHeader(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public RestClient withHeader(String name, String value) {
        // The `host` header cannot be set explicitly in Java 11, but it is still used
        //  as part of request preparation (AWS signing, etc.), so the most thorough
        //  solution is to clean it before request sending
        if (name.equalsIgnoreCase("host")) {
            return this;
        }
        this.headers.add(new Parameter(name, value));
        return this;
    }

    public RestClient withBody(String body) {
        this.body = body;
        return this;
    }

    public RestClient withRequestTimeoutSeconds(int timeoutSeconds) {
        this.requestTimeoutSeconds = timeoutSeconds;
        return this;
    }

    public RestClient withRetries(int retries) {
        this.retries = retries;
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
        return callWithRetries(buildHttpRequest("GET"));
    }

    public Response post() {
        return callWithRetries(buildHttpRequest("POST"));
    }

    public Response put() {
        return callWithRetries(buildHttpRequest("PUT"));
    }

    private HttpRequest buildHttpRequest(String method) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url));

        if (requestTimeoutSeconds > 0) {
            builder.timeout(Duration.ofSeconds(requestTimeoutSeconds));
        }
        headers.forEach(parameter -> builder.header(parameter.getKey(), parameter.getValue()));

        HttpRequest.BodyPublisher publisher = body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);

        // To emulate HttpURLConnection behaviour, which sets the method to "POST"
        // if a body is provided, even after the requested method was set to "GET"
        String finalMethod = body != null && "GET".equals(method) ? "POST" : method;

        return builder.method(finalMethod, publisher)
                .build();
    }

    private Response callWithRetries(HttpRequest request) {
        return RetryUtils.retry(() -> call(request), retries);
    }

    private Response call(HttpRequest request) {
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            checkResponseCode(request.method(), response);
            return new Response(response.statusCode(), response.body());
        } catch (IOException e) {
            throw new RestClientException("Failure in executing REST call", e);
        } catch (InterruptedException e) {
            throw new RestClientException("REST call interrupted", e);
        }
    }

    /**
     * Issues a watch request to a Kubernetes resource, starting with the given {@code resourceVersion}.
     * Since a watch implies a stream of updates from the server will be consumed, unlike other methods
     * in this class, it is the responsibility of the consumer to disconnect the connection
     * (by invoking {@link WatchResponse#disconnect()}) once the watch is no longer required.
     */
    public WatchResponse watch(String resourceVersion) throws RestClientException {
        try {
            String appendWatchParameter = (url.contains("?") ? "&" : "?") + String.format(WATCH_FORMAT, resourceVersion);
            String completeUrl = url + appendWatchParameter;
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(completeUrl))
                    .GET();

            if (requestTimeoutSeconds > 0) {
                requestBuilder.timeout(Duration.ofSeconds(requestTimeoutSeconds));
            }

            for (Parameter header : headers) {
                requestBuilder.header(header.getKey(), header.getValue());
            }

            if (body != null) {
                byte[] bodyData = body.getBytes(StandardCharsets.UTF_8);
                requestBuilder.setHeader("Content-Length", String.valueOf(bodyData.length));
                requestBuilder.setHeader("charset", "utf-8");
                requestBuilder.method("GET", HttpRequest.BodyPublishers.ofByteArray(bodyData));
            }

            HttpRequest request = requestBuilder.build();
            HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

            checkResponseCode(request.method(), response);
            return new WatchResponse(response);
        } catch (IOException e) {
            throw new RestClientException("Failure in executing REST call", e);
        } catch (InterruptedException e) {
            throw new RestClientException("REST call interrupted", e);
        }
    }

    private void checkResponseCode(String method, HttpResponse<?> response) {
        int responseCode = response.statusCode();
        if (!isExpectedResponseCode(responseCode)) {
            String errorMessage = "none, body type: " + response.body().getClass();
            if (response.body() instanceof String) {
                errorMessage = (String) response.body();
            } else if (response.body() instanceof InputStream) {
                Scanner scanner = new Scanner((InputStream) response.body(), StandardCharsets.UTF_8);
                scanner.useDelimiter("\\Z");
                if (scanner.hasNext()) {
                    errorMessage = scanner.next();
                }
            }
            throw new RestClientException(
                    String.format("Failure executing: %s at: %s. Message: %s", method, url, errorMessage),
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
     * Builds SSL Context with the public CA Certificate from Kubernetes Master.
     */
    private SSLContext buildSslContext(String caCertificate) {
        try {
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);

            int i = 0;
            for (Certificate certificate : generateCertificates(caCertificate)) {
                String alias = String.format("ca-%d", i++);
                keyStore.setCertificateEntry(alias, certificate);
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);

            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, tmf.getTrustManagers(), null);
            return context;

        } catch (Exception e) {
            throw new RestClientException("Failure in generating SSLSocketFactory for certificate " + caCertificate, e);
        }
    }

    /**
     * Generates CA Certificate from the default CA Cert file or from the externally provided "ca-certificate" property.
     */
    private Collection<? extends Certificate> generateCertificates(String caCertificate) throws CertificateException {
        InputStream caInput = null;
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            caInput = new ByteArrayInputStream(caCertificate.getBytes(StandardCharsets.UTF_8));
            return cf.generateCertificates(caInput);
        } finally {
            IOUtil.closeResource(caInput);
        }
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

    public static class WatchResponse {
        private final int code;
        private final HttpResponse<InputStream> response;
        private final BufferedReader reader;

        public WatchResponse(HttpResponse<InputStream> response) throws IOException {
            this.code = response.statusCode();
            this.response = response;
            this.reader = new BufferedReader(new InputStreamReader(response.body()));
        }

        public int getCode() {
            return code;
        }

        public String nextLine() throws IOException {
            return reader.readLine();
        }

        public void disconnect() throws IOException {
            response.body().close();
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
