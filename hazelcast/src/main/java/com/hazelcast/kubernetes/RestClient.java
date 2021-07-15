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

package com.hazelcast.kubernetes;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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
import java.util.Collection;
import java.util.List;
import java.util.Scanner;

/**
 * Utility class for making REST calls.
 */
final class RestClient {
    private static final ILogger LOGGER = Logger.getLogger(RestClient.class);

    private static final int HTTP_OK = 200;

    private final String url;
    private final List<Header> headers = new ArrayList<Header>();
    private String body;
    private String caCertificate;

    private RestClient(String url) {
        this.url = url;
    }

    static RestClient create(String url) {
        return new RestClient(url);
    }

    RestClient withHeader(String key, String value) {
        headers.add(new Header(key, value));
        return this;
    }

    RestClient withBody(String body) {
        this.body = body;
        return this;
    }

    RestClient withCaCertificates(String caCertificate) {
        this.caCertificate = caCertificate;
        return this;
    }

    String get() {
        return call("GET");
    }

    String post() {
        return call("POST");
    }

    private String call(String method) {
        HttpURLConnection connection = null;
        DataOutputStream outputStream = null;
        try {
            URL urlToConnect = new URL(url);
            connection = (HttpURLConnection) urlToConnect.openConnection();
            if (connection instanceof HttpsURLConnection) {
                ((HttpsURLConnection) connection).setSSLSocketFactory(buildSslSocketFactory());
            }
            connection.setRequestMethod(method);
            for (Header header : headers) {
                connection.setRequestProperty(header.getKey(), header.getValue());
            }
            if (body != null) {
                byte[] bodyData = body.getBytes(StandardCharsets.UTF_8);

                connection.setDoOutput(true);
                connection.setRequestProperty("charset", "utf-8");
                connection.setRequestProperty("Content-Length", Integer.toString(bodyData.length));

                outputStream = new DataOutputStream(connection.getOutputStream());
                outputStream.write(bodyData);
                outputStream.flush();
            }

            checkHttpOk(method, connection);
            return read(connection.getInputStream());
        } catch (IOException e) {
            throw new RestClientException("Failure in executing REST call", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    LOGGER.finest("Error while closing HTTP output stream", e);
                }
            }
        }
    }

    private void checkHttpOk(String method, HttpURLConnection connection)
            throws IOException {
        if (connection.getResponseCode() != HTTP_OK) {
            String errorMessage;
            try {
                errorMessage = read(connection.getErrorStream());
            } catch (Exception e) {
                throw new RestClientException(
                        String.format("Failure executing: %s at: %s", method, url), connection.getResponseCode());
            }
            throw new RestClientException(String.format("Failure executing: %s at: %s. Message: %s", method, url, errorMessage),
                    connection.getResponseCode());

        }
    }

    private static String read(InputStream stream) {
        if (stream == null) {
            return "";
        }
        Scanner scanner = new Scanner(stream, "UTF-8");
        scanner.useDelimiter("\\Z");
        return scanner.next();
    }

    private static final class Header {
        private final String key;
        private final String value;

        private Header(String key, String value) {
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
            throw new KubernetesClientException("Failure in generating SSLSocketFactory", e);
        }
    }

    /**
     * Generates CA Certificate from the default CA Cert file or from the externally provided "ca-certificate" property.
     */
    private Collection<? extends Certificate> generateCertificates()
            throws IOException, CertificateException {
        InputStream caInput = null;
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            caInput = new ByteArrayInputStream(caCertificate.getBytes(StandardCharsets.UTF_8));
            return cf.generateCertificates(caInput);
        } finally {
            IOUtil.closeResource(caInput);
        }
    }
}
