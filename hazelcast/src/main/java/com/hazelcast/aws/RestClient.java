/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aws;

import com.hazelcast.spi.exception.RestClientException;
import com.hazelcast.spi.utils.RetryUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

final class RestClient {

    static final int HTTP_OK = 200;
    static final int HTTP_NOT_FOUND = 404;

    private final String url;
    private final List<Parameter> headers = new ArrayList<>();
    private Set<Integer> expectedResponseCodes;
    private String body;
    private int readTimeoutSeconds;
    private int connectTimeoutSeconds;
    private int retries;

    private RestClient(String url) {
        this.url = url;
    }

    static RestClient create(String url) {
        return new RestClient(url);
    }

    RestClient withHeaders(Map<String, String> headers) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            this.headers.add(new Parameter(entry.getKey(), entry.getValue()));
        }
        return this;
    }

    RestClient withBody(String body) {
        this.body = body;
        return this;
    }

    RestClient withReadTimeoutSeconds(int readTimeoutSeconds) {
        this.readTimeoutSeconds = readTimeoutSeconds;
        return this;
    }

    RestClient withConnectTimeoutSeconds(int connectTimeoutSeconds) {
        this.connectTimeoutSeconds = connectTimeoutSeconds;
        return this;
    }

    RestClient withRetries(int retries) {
        this.retries = retries;
        return this;
    }

    RestClient expectResponseCodes(Integer... codes) {
        if (expectedResponseCodes == null) {
            expectedResponseCodes = new HashSet<>();
        }
        expectedResponseCodes.addAll(Arrays.asList(codes));
        return this;
    }

    Response get() {
        return callWithRetries("GET");
    }

    Response post() {
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

    static class Response {

        private final int code;
        private final String body;

        Response(int code, String body) {
            this.code = code;
            this.body = body;
        }

        int getCode() {
            return code;
        }

        String getBody() {
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
