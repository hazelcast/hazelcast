/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.rest;

import com.hazelcast.spi.annotation.Beta;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * This class allows controlling the Hazelcast REST API feature.
 *
 * @since 5.4
 */
@Beta
public class RestConfig {
    public static class Ssl {

        private boolean enabled;

        private ClientAuth clientAuth = ClientAuth.NONE;

        private String ciphers;

        private String enabledProtocols;

        private String keyAlias;

        private String keyPassword;

        private String keyStore;

        private String keyStorePassword;

        private String keyStoreType;

        private String keyStoreProvider;

        private String trustStore;

        private String trustStorePassword;

        private String trustStoreType;

        private String trustStoreProvider;

        private String certificate;

        private String certificatePrivateKey;

        private String trustCertificate;

        private String trustCertificatePrivateKey;

        private String protocol = "TLS";

        /**
         * Return whether to enable SSL support.
         *
         * @return whether to enable SSL support
         */
        public boolean isEnabled() {
            return this.enabled;
        }

        /**
         * Set whether to enable SSL support.
         *
         * @param enabled whether to enable SSL support
         * @return the {@link Ssl} to use
         */
        public Ssl setEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /**
         * Return Whether client authentication is not wanted ("none"), wanted ("want") or
         * needed ("need"). Requires a trust store.
         *
         * @return the {@link ClientAuth} to use
         */
        public ClientAuth getClientAuth() {
            return this.clientAuth;
        }

        /**
         * Set whether client authentication is not wanted ("none"), wanted ("want") or
         * needed ("need"). Requires a trust store.
         *
         * @param clientAuth the authentication mode
         * @return the {@link Ssl} to use
         */
        public Ssl setClientAuth(ClientAuth clientAuth) {
            this.clientAuth = clientAuth;
            return this;
        }

        /**
         * Return the supported SSL ciphers.
         *
         * @return Comma separated list of the supported SSL ciphers list
         */
        public String getCiphers() {
            return this.ciphers;
        }

        /**
         * Set the supported SSL ciphers.
         *
         * @param ciphers Comma separated list of supported SSL ciphers
         * @return the {@link Ssl} to use
         */
        public Ssl setCiphers(String ciphers) {
            this.ciphers = ciphers;
            return this;
        }

        /**
         * Return the enabled SSL protocols.
         *
         * @return Comma separated list of the enabled SSL protocols.
         */
        public String getEnabledProtocols() {
            return this.enabledProtocols;
        }

        /**
         * Set the enabled SSL protocols.
         *
         * @param enabledProtocols Comma separated list of the enabled SSL protocols
         * @return the {@link Ssl} to use
         */
        public Ssl setEnabledProtocols(String enabledProtocols) {
            this.enabledProtocols = enabledProtocols;
            return this;
        }

        /**
         * Return the alias that identifies the key in the key store.
         *
         * @return the key alias
         */
        public String getKeyAlias() {
            return this.keyAlias;
        }

        /**
         * Set the alias that identifies the key in the key store.
         *
         * @param keyAlias the key alias
         * @return the {@link Ssl} to use
         */
        public Ssl setKeyAlias(String keyAlias) {
            this.keyAlias = keyAlias;
            return this;
        }

        /**
         * Return the password used to access the key in the key store.
         *
         * @return the key password
         */
        public String getKeyPassword() {
            return this.keyPassword;
        }

        /**
         * Set the password used to access the key in the key store.
         *
         * @param keyPassword the key password
         * @return the {@link Ssl} to use
         */
        public Ssl setKeyPassword(String keyPassword) {
            this.keyPassword = keyPassword;
            return this;
        }

        /**
         * Return the path to the key store that holds the SSL certificate (typically a jks
         * file).
         *
         * @return the path to the key store
         */
        public String getKeyStore() {
            return this.keyStore;
        }

        /**
         * Set the path to the key store that holds the SSL certificate (typically a jks
         * file).
         *
         * @param keyStore the path to the key store
         * @return the {@link Ssl} to use
         */
        public Ssl setKeyStore(String keyStore) {
            this.keyStore = keyStore;
            return this;
        }

        /**
         * Return the password used to access the key store.
         *
         * @return the key store password
         */
        public String getKeyStorePassword() {
            return this.keyStorePassword;
        }

        /**
         * Set the password used to access the key store.
         *
         * @param keyStorePassword the key store password
         * @return the {@link Ssl} to use
         */
        public Ssl setKeyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        /**
         * Return the type of the key store.
         *
         * @return the key store type
         */
        public String getKeyStoreType() {
            return this.keyStoreType;
        }

        /**
         * Set the type of the key store.
         *
         * @param keyStoreType the key store type. Can be JKS or PKCS12.
         * @return the {@link Ssl} to use
         */
        public Ssl setKeyStoreType(String keyStoreType) {
            this.keyStoreType = keyStoreType;
            return this;
        }

        /**
         * Return the provider for the key store.
         *
         * @return the key store provider
         */
        public String getKeyStoreProvider() {
            return this.keyStoreProvider;
        }

        public Ssl setKeyStoreProvider(String keyStoreProvider) {
            this.keyStoreProvider = keyStoreProvider;
            return this;
        }

        /**
         * Return the trust store that holds SSL certificates.
         *
         * @return the trust store
         */
        public String getTrustStore() {
            return this.trustStore;
        }

        /**
         * Set the trust store that holds SSL certificates.
         *
         * @param trustStore the trust store
         * @return the {@link Ssl} to use
         */
        public Ssl setTrustStore(String trustStore) {
            this.trustStore = trustStore;
            return this;
        }

        /**
         * Return the password used to access the trust store.
         *
         * @return the trust store password
         */
        public String getTrustStorePassword() {
            return this.trustStorePassword;
        }

        /**
         * Set the password used to access the trust store.
         *
         * @param trustStorePassword the trust store password
         * @return the {@link Ssl} to use
         */
        public Ssl setTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        /**
         * Return the type of the trust store.
         *
         * @return the trust store type
         */
        public String getTrustStoreType() {
            return this.trustStoreType;
        }

        /**
         * Set the type of the trust store.
         *
         * @param trustStoreType the trust store type. Can be JKS or PKCS12.
         * @return the {@link Ssl} to use
         */
        public Ssl setTrustStoreType(String trustStoreType) {
            this.trustStoreType = trustStoreType;
            return this;
        }

        /**
         * Return the provider for the trust store.
         *
         * @return the trust store provider
         */
        public String getTrustStoreProvider() {
            return this.trustStoreProvider;
        }


        /**
         * Set the provider for the trust store.
         *
         * @param trustStoreProvider the trust store provider
         * @return the {@link Ssl} to use
         */
        public Ssl setTrustStoreProvider(String trustStoreProvider) {
            this.trustStoreProvider = trustStoreProvider;
            return this;
        }

        /**
         * Return the location of the certificate in PEM format.
         *
         * @return the certificate location
         */
        public String getCertificate() {
            return this.certificate;
        }

        /**
         * Set the location of the certificate in PEM format.
         *
         * @param certificate the certificate location
         * @return the {@link Ssl} to use
         */
        public Ssl setCertificate(String certificate) {
            this.certificate = certificate;
            return this;
        }

        /**
         * Return the location of the private key for the certificate in PEM format.
         *
         * @return the location of the certificate private key
         */
        public String getCertificatePrivateKey() {
            return this.certificatePrivateKey;
        }

        /**
         * Set the location of the private key for the certificate in PEM format.
         *
         * @param certificatePrivateKey the location of the certificate private key
         * @return the {@link Ssl} to use
         */
        public Ssl setCertificatePrivateKey(String certificatePrivateKey) {
            this.certificatePrivateKey = certificatePrivateKey;
            return this;
        }

        /**
         * Return the location of the trust certificate authority chain in PEM format.
         *
         * @return the location of the trust certificate
         */
        public String getTrustCertificate() {
            return this.trustCertificate;
        }

        /**
         * Set the location of the trust certificate authority chain in PEM format.
         *
         * @param trustCertificate the location of the trust certificate
         * @return the {@link Ssl} to use
         */
        public Ssl setTrustCertificate(String trustCertificate) {
            this.trustCertificate = trustCertificate;
            return this;
        }

        /**
         * Return the location of the private key for the trust certificate in PEM format.
         *
         * @return the location of the trust certificate private key
         */
        public String getTrustCertificatePrivateKey() {
            return this.trustCertificatePrivateKey;
        }

        /**
         * Set the location of the private key for the trust certificate in PEM format.
         *
         * @param trustCertificatePrivateKey the location of the trust certificate private key
         * @return the {@link Ssl} to use
         */
        public Ssl setTrustCertificatePrivateKey(String trustCertificatePrivateKey) {
            this.trustCertificatePrivateKey = trustCertificatePrivateKey;
            return this;
        }

        /**
         * Return the SSL protocol to use.
         *
         * @return the SSL protocol
         */
        public String getProtocol() {
            return this.protocol;
        }

        /**
         * Set the SSL protocol to use.
         *
         * @param protocol the SSL protocol
         * @return the {@link Ssl} to use
         */
        public Ssl setProtocol(String protocol) {
            this.protocol = protocol;
            return this;
        }

        /**
         * Client authentication types.
         */
        public enum ClientAuth {

            /**
             * Client authentication is not wanted.
             */
            NONE,

            /**
             * Client authentication is wanted but not mandatory.
             */
            WANT,

            /**
             * Client authentication is needed and mandatory.
             */
            NEED

        }

        @Override
        public String toString() {
            return "Ssl{"
                    + "enabled=" + enabled
                    + ", clientAuth=" + clientAuth
                    + ", ciphers=" + ciphers
                    + ", enabledProtocols=" + enabledProtocols
                    + ", keyAlias='" + keyAlias + '\''
                    + ", keyPassword='" + keyPassword + '\''
                    + ", keyStore='" + keyStore + '\''
                    + ", keyStorePassword='" + keyStorePassword + '\''
                    + ", keyStoreType='" + keyStoreType + '\''
                    + ", keyStoreProvider='" + keyStoreProvider + '\''
                    + ", trustStore='" + trustStore + '\''
                    + ", trustStorePassword='" + trustStorePassword + '\''
                    + ", trustStoreType='" + trustStoreType + '\''
                    + ", trustStoreProvider='" + trustStoreProvider + '\''
                    + ", certificate='" + certificate + '\''
                    + ", certificatePrivateKey='" + certificatePrivateKey + '\''
                    + ", trustCertificate='" + trustCertificate + '\''
                    + ", trustCertificatePrivateKey='" + trustCertificatePrivateKey + '\''
                    + ", protocol='" + protocol + '\''
                    + '}';
        }
    }

    private static final int DEFAULT_PORT = 8443;
    private static final int DEFAULT_DURATION_MINUTES = 15;
    private static final Duration DEFAULT_DURATION = Duration.of(DEFAULT_DURATION_MINUTES, ChronoUnit.MINUTES);

    /**
     * Indicates whether the RestConfig is enabled.
     */
    private boolean enabled;

    /**
     * The port number for the Rest API server endpoint.
     */
    private int port = DEFAULT_PORT;

    /**
     * The name of the Rest security realm which should be already configured.
     */
    private String securityRealm;

    /**
     * Duration for a token to remain valid.
     */
    private Duration tokenValidityDuration = DEFAULT_DURATION;

    /**
     * SSL configuration.
     */
    private Ssl ssl = new Ssl();

    /**
     * Default constructor for RestConfig.
     */
    public RestConfig() {
    }

    /**
     * Checks if the RestConfig is enabled.
     *
     * @return true if the RestConfig is enabled, false otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the enabled status of the RestConfig.
     *
     * @param enabled the new enabled status.
     * @return the updated RestConfig.
     */
    public RestConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Gets the port of the RestConfig.
     *
     * @return the port of the RestConfig.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port of the RestConfig.
     *
     * @param port the new port.
     * @return the updated RestConfig.
     */
    public RestConfig setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * Gets the name of the Rest security realm.
     *
     * @return the name of the realm.
     */
    public String getSecurityRealm() {
        return securityRealm;
    }

    /**
     * Sets the name of the Rest security realm.
     *
     * @param securityRealm the name of the realm. This should be an already defined valid security realm.
     */
    public RestConfig setSecurityRealm(String securityRealm) {
        this.securityRealm = securityRealm;
        return this;
    }

    /**
     * Gets the token validity duration.
     *
     * @return the duration for which the token is valid.
     */
    public Duration getTokenValidityDuration() {
        return tokenValidityDuration;
    }

    /**
     * Sets the expiration duration for jwt token.
     * <b>WARNING:</b> The resolution for tokenValidityDuration can not be more than a second.
     *
     * @param tokenValidityDuration the duration for which the token should be valid.
     */
    public RestConfig setTokenValidityDuration(Duration tokenValidityDuration) {
        this.tokenValidityDuration = tokenValidityDuration;
        return this;
    }

    /**
     * Gets the SSL configuration.
     *
     * @return the SSL configuration.
     */
    public Ssl getSsl() {
        return ssl;
    }

    /**
     * Sets the SSL configuration.
     *
     * @param ssl the new SSL configuration.
     * @return the updated RestConfig.
     */
    public RestConfig setSsl(Ssl ssl) {
        this.ssl = ssl;
        return this;
    }

    /**
     * Returns a string representation of the RestConfig.
     *
     * @return a string representation of the RestConfig.
     */
    @Override
    public String toString() {
        return "RestConfig{enabled=" + enabled + ", port=" + port + ", securityRealm='" + securityRealm + '\''
                + ", tokenValidityDuration=" + tokenValidityDuration + ", ssl=" + ssl + '}';
    }
}
