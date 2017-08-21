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

package com.hazelcast.nio.ssl;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Properties;

import static com.hazelcast.nio.IOUtil.closeResource;

/**
 * A support class for {@link SSLEngineFactory} and {@link SSLContextFactory} implementation that takes care of
 * the logic for KeyManager and TrustManager.
 */
public abstract class SSLEngineFactorySupport {

    public static final String JAVA_NET_SSL_PREFIX = "javax.net.ssl.";

    protected KeyManagerFactory kmf;
    protected TrustManagerFactory tmf;
    protected String protocol;

    protected void load(Properties properties) throws Exception {
        String keyStorePassword = getProperty(properties, "keyStorePassword");
        String keyStore = getProperty(properties, "keyStore");
        String trustStore = getProperty(properties, "trustStore", keyStore);
        String trustStorePassword = getProperty(properties, "trustStorePassword", keyStorePassword);
        String keyManagerAlgorithm = properties.getProperty("keyManagerAlgorithm", KeyManagerFactory.getDefaultAlgorithm());
        String trustManagerAlgorithm = properties.getProperty("trustManagerAlgorithm", TrustManagerFactory.getDefaultAlgorithm());
        this.protocol = properties.getProperty("protocol", "TLS");
        this.kmf = loadKeyManagerFactory(keyStorePassword, keyStore, keyManagerAlgorithm);
        this.tmf = loadTrustManagerFactory(trustStorePassword, trustStore, trustManagerAlgorithm);
    }

    public static TrustManagerFactory loadTrustManagerFactory(String trustStorePassword,
                                                              String trustStore,
                                                              String trustManagerAlgorithm) throws Exception {
        if (trustStore == null) {
            return null;
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(trustManagerAlgorithm);
        char[] passPhrase = trustStorePassword == null ? null : trustStorePassword.toCharArray();
        KeyStore ts = KeyStore.getInstance("JKS");
        loadKeyStore(ts, passPhrase, trustStore);
        tmf.init(ts);
        return tmf;
    }

    public static KeyManagerFactory loadKeyManagerFactory(String keyStorePassword,
                                                          String keyStore,
                                                          String keyManagerAlgorithm) throws Exception {
        if (keyStore == null) {
            return null;
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyManagerAlgorithm);
        char[] passPhrase = keyStorePassword == null ? null : keyStorePassword.toCharArray();
        KeyStore ks = KeyStore.getInstance("JKS");
        loadKeyStore(ks, passPhrase, keyStore);
        kmf.init(ks, passPhrase);
        return kmf;
    }

    public static void loadKeyStore(KeyStore ks, char[] passPhrase, String keyStoreFile)
            throws IOException, NoSuchAlgorithmException, CertificateException {
        InputStream in = new FileInputStream(keyStoreFile);
        try {
            ks.load(in, passPhrase);
        } finally {
            closeResource(in);
        }
    }

    protected static String getProperty(Properties properties, String property) {
        String value = properties.getProperty(property);
        if (value == null) {
            value = properties.getProperty(JAVA_NET_SSL_PREFIX + property);
        }
        if (value == null) {
            value = System.getProperty(JAVA_NET_SSL_PREFIX + property);
        }
        return value;
    }

    protected static String getProperty(Properties properties, String property, String defaultValue) {
        String value = getProperty(properties, property);
        return value != null ? value : defaultValue;
    }
}
