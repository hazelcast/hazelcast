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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.spi.utils.AbstractRestClient;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Collection;

/**
 * Utility class for making REST calls.
 */
final class KubernetesRestClient extends AbstractRestClient<KubernetesRestClient> {

    private String caCertificate;

    private KubernetesRestClient(String url) {
        super(url);
    }

    @Override
    protected HttpURLConnection buildHttpConnection(URL urlToConnect) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) urlToConnect.openConnection();
        if (connection instanceof HttpsURLConnection) {
            ((HttpsURLConnection) connection).setSSLSocketFactory(buildSslSocketFactory());
        }
        return connection;
    }

    @Override
    protected KubernetesRestClient self() {
        return this;
    }

    static KubernetesRestClient create(String url) {
        return new KubernetesRestClient(url);
    }

    KubernetesRestClient withCaCertificates(String caCertificate) {
        this.caCertificate = caCertificate;
        return this;
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
}
