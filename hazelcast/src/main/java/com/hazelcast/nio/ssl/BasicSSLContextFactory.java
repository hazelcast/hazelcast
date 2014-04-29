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

package com.hazelcast.nio.ssl;

import com.hazelcast.nio.IOUtil;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Properties;

public class BasicSSLContextFactory implements SSLContextFactory {

    private static final String JAVA_NET_SSL_PREFIX = "javax.net.ssl.";

    private SSLContext sslContext;

    public BasicSSLContextFactory() {
    }

    public void init(Properties properties) throws Exception {
        KeyStore ks = KeyStore.getInstance("JKS");
        KeyStore ts = KeyStore.getInstance("JKS");
        String keyStorePassword = getProperty(properties, "keyStorePassword");
        String keyStore = getProperty(properties, "keyStore");
        if (keyStore == null || keyStorePassword == null) {
            throw new RuntimeException("SSL is enabled but keyStore[Password] properties aren't set!");
        }
        String trustStore = getProperty(properties, "trustStore", keyStore);
        String trustStorePassword = getProperty(properties, "trustStorePassword", keyStorePassword);

        String keyManagerAlgorithm = properties.getProperty("keyManagerAlgorithm", KeyManagerFactory.getDefaultAlgorithm());
        String trustManagerAlgorithm = properties.getProperty("trustManagerAlgorithm", TrustManagerFactory.getDefaultAlgorithm());
        String protocol = properties.getProperty("protocol", "TLS");

        final char[] keyStorePassPhrase = keyStorePassword.toCharArray();
        loadKeyStore(ks, keyStorePassPhrase, keyStore);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyManagerAlgorithm);
        kmf.init(ks, keyStorePassPhrase);

        loadKeyStore(ts, trustStorePassword.toCharArray(), trustStore);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(trustManagerAlgorithm);
        tmf.init(ts);

        sslContext = SSLContext.getInstance(protocol);
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    }

    private void loadKeyStore(KeyStore ks, char[] passPhrase, String keyStoreFile)
            throws IOException, NoSuchAlgorithmException, CertificateException {
        final InputStream in = new FileInputStream(keyStoreFile);
        try {
            ks.load(in, passPhrase);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private static String getProperty(Properties properties, String property) {
        String value = properties.getProperty(property);
        if (value == null) {
            value = properties.getProperty(JAVA_NET_SSL_PREFIX + property);
        }
        if (value == null) {
            value = System.getProperty(JAVA_NET_SSL_PREFIX + property);
        }
        return value;
    }

    private static String getProperty(Properties properties, String property, String defaultValue) {
        String value = getProperty(properties, property);
        return value != null ? value : defaultValue;
    }

    public SSLContext getSSLContext() {
        return sslContext;
    }
}
