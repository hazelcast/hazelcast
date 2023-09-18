/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.mongodb.dataconnection;

import com.hazelcast.config.DataConnectionConfig;
import com.mongodb.connection.SslSettings.Builder;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.mongodb.dataconnection.MongoDataConnection.allSame;

class SslConf {

    /**
     * Name of the property holding the information if SSL should be enabled for clients.
     *
     * Default value is false.
     *
     * @since 5.4
     * @see com.mongodb.connection.SslSettings.Builder#enabled(boolean)
     */
    public static final String ENABLE_SSL = "enableSsl";

    /**
     * Name of the property holding information if invalid host names will be allowed.
     *
     * Default value is false.
     *
     * @since 5.4
     * @see com.mongodb.connection.SslSettings.Builder#invalidHostNameAllowed(boolean)
     */
    public static final String INVALID_HOSTNAME_ALLOWED = "invalidHostNameAllowed";

    /**
     * Name of the property holding keystore location. Keystore must be in the same
     * location on every cluster member.
     *
     * @since 5.4
     */
    public static final String KEYSTORE_LOCATION = "keyStore";

    /**
     * Name of the property holding keystore type.
     *
     * If it's null it will default to system's default keystore type.
     *
     * @since 5.4
     */
    public static final String KEYSTORE_TYPE = "keyStoreType";

    /**
     * Name of the property holding keystore password.
     *
     * @since 5.4
     */
    public static final String KEYSTORE_PASSWORD = "keyStorePassword";

    /**
     * Name of the property holding keystore location. Keystore must be in the same
     * location on every cluster member.
     *
     * @since 5.4
     */
    public static final String TRUSTSTORE_LOCATION = "trustStore";

    /**
     * Name of the property holding truststore type.
     *
     * If it's null it will default to system's default keystore type.
     *
     * @since 5.4
     */
    public static final String TRUSTSTORE_TYPE = "trustStoreType";

    /**
     * Name of the property holding truststore password.
     *
     * @since 5.4
     */
    public static final String TRUSTSTORE_PASSWORD = "trustStorePassword";

    private final boolean enableSsl;
    private final boolean invalidHostNameAllowed;
    private final String keyStoreLocation;
    private final String keyStoreType;
    private final String keyStorePassword;
    private final String trustStoreLocation;
    private final String trustStoreType;
    private final String trustStorePassword;

    SslConf(DataConnectionConfig config) {
        this.enableSsl = Boolean.parseBoolean(config.getProperty(ENABLE_SSL, "false"));
        this.invalidHostNameAllowed = Boolean.parseBoolean(config.getProperty(INVALID_HOSTNAME_ALLOWED, "false"));
        this.keyStoreLocation = config.getProperty(KEYSTORE_LOCATION);
        this.keyStoreType = config.getProperty(KEYSTORE_TYPE);
        this.keyStorePassword = config.getProperty(KEYSTORE_PASSWORD);
        this.trustStoreLocation = config.getProperty(TRUSTSTORE_LOCATION);
        this.trustStoreType = config.getProperty(TRUSTSTORE_TYPE);
        this.trustStorePassword = config.getProperty(TRUSTSTORE_PASSWORD);

        checkState(allSame((keyStoreLocation == null), (keyStorePassword == null)),
                "KeyStore configuration is not full, you must provide both keyStore location and password");
        checkState(allSame((trustStoreLocation == null), (trustStorePassword == null)),
                "KeyStore configuration is not full, you must provide both trustStore location and password");
    }

    public void apply(Builder builder) {
        builder.enabled(enableSsl);
        builder.invalidHostNameAllowed(invalidHostNameAllowed);

        if (keyStoreLocation != null || trustStoreLocation != null) { // key stores configured, see constructor invariants
            char[] ksPass = keyStorePassword == null ? null : keyStorePassword.toCharArray();
            char[] tsPass = trustStorePassword == null ? null : trustStorePassword.toCharArray();

            SSLContext sslContext = createSSLContext(keyStoreLocation, keyStoreType, ksPass,
                    trustStoreLocation, trustStoreType, tsPass);
            builder.context(sslContext);
        }
    }

    static SSLContext createSSLContext(String ksFile, String ksType, char[] ksPass,
                                               String tsFile, String tsType, char[] tsPass) {
        try {
            KeyStore keyStore = loadKeystore(ksFile, ksType, ksPass);
            KeyManagerFactory keyManagerFactory = null;
            if (keyStore != null) {
                keyManagerFactory = KeyManagerFactory.getInstance("PKIX");
                keyManagerFactory.init(keyStore, ksPass);
            }
            KeyStore trustStore = loadKeystore(tsFile, tsType, tsPass);
            TrustManagerFactory trustManagerFactory = null;
            if (trustStore != null) {
                trustManagerFactory = TrustManagerFactory.getInstance("PKIX");
                trustManagerFactory.init(trustStore);
            }
            SSLContext sslContext = SSLContext.getInstance("TLS");
            KeyManager[] keyManagers = keyManagerFactory == null ? null : keyManagerFactory.getKeyManagers();
            TrustManager[] trustManagers = trustManagerFactory == null ? null : trustManagerFactory.getTrustManagers();

            sslContext.init(keyManagers, trustManagers, null);
            return sslContext;
        } catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException("Cannot configure SSL", e);
        }
    }

    private static KeyStore loadKeystore(String ksFile, String ksType, char[] ksPass)
            throws IOException, GeneralSecurityException {
        if (ksFile == null) {
            return null;
        }
        KeyStore ks = KeyStore.getInstance(ksType == null ? KeyStore.getDefaultType() : ksType);
        try (InputStream stream = new FileInputStream(ksFile)) {
            ks.load(stream, ksPass);
        }
        return ks;
    }

}
