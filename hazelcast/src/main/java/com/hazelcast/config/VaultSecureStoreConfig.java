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

package com.hazelcast.config;

import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * HashiCorp Vault Secure Store configuration.
 * <p>
 * The Vault Secure Store uses the Vault REST API to communicate with Vault. The relevant
 * configuration properties are the Vault REST server address; the secret path; the
 * authentication token; and, optionally, the SSL/TLS configuration for HTTPS support.
 * <p>
 * Only the KV secrets engine (see https://www.vaultproject.io/docs/secrets/kv/index.html)
 * is supported.
 * <p>
 * The encryption key is expected to be stored at the specified secret path and represented
 * as a single key/value pair in the following format:
 * <pre>
 * name=Base64-encoded-data
 * </pre>
 * where {@code name} can be an arbitrary string. Multiple key/value pairs under the same
 * secret path are not supported.
 * <p>
 * If KV secrets engine V2 is used, the Vault Secure Store is able to retrieve the
 * available previous versions of the encryption keys.
 * <p>
 * Changes to the encryption key can be detected automatically if polling
 * (see {@link #setPollingInterval(int)}) is enabled.
 */
public class VaultSecureStoreConfig extends SecureStoreConfig {

    /**
     * Default interval (in seconds) for polling for changes to the encryption key: 0
     * (polling disabled).
     */
    public static final int DEFAULT_POLLING_INTERVAL = 0;

    private String address;
    private String secretPath;
    private String token;
    private SSLConfig sslConfig;
    private int pollingInterval = DEFAULT_POLLING_INTERVAL;

    /**
     * Creates a new Vault Secure Store configuration.
     * @param address the Vault server address
     * @param secretPath the secret path
     * @param token the access token
     */
    public VaultSecureStoreConfig(String address, String secretPath, String token) {
        checkNotNull(address, "Vault server address cannot be null!");
        checkNotNull(secretPath, "Vault secret path cannot be null!");
        checkNotNull(token, "Vault token cannot be null!");
        this.address = address;
        this.secretPath = secretPath;
        this.token = token;
    }
    /**
     * Returns the Vault server address.
     *
     * @return the Vault server address
     */
    public String getAddress() {
        return address;
    }

    /**
     * Sets the Vault server address.
     *
     * @param address the Vault server address
     */
    public VaultSecureStoreConfig setAddress(String address) {
        checkNotNull(address, "Vault server address cannot be null!");
        this.address = address;
        return this;
    }

    /**
     * Returns the Vault access token.
     *
     * @return the Vault access token
     */
    public String getToken() {
        return token;
    }

    /**
     * Sets the Vault access token.
     *
     * @param token the access token
     * @return the updated {@link VaultSecureStoreConfig} instance
     * @throws IllegalArgumentException if token is {code null}
     */
    public VaultSecureStoreConfig setToken(String token) {
        checkNotNull(token, "Vault token cannot be null!");
        this.token = token;
        return this;
    }

    /**
     * Returns the Vault secret path.
     *
     * @see #setSecretPath(String)
     * @return the Vault secret path
     */
    public String getSecretPath() {
        return secretPath;
    }

    /**
     * Sets the Vault secret path where the encryption keys is expected to be stored.
     *
     * @param secretPath the secret path
     * @return the updated {@link VaultSecureStoreConfig} instance
     * @throws IllegalArgumentException if secretPath is {code null}
     */
    public VaultSecureStoreConfig setSecretPath(String secretPath) {
        checkNotNull(secretPath, "Vault secret path cannot be null!");
        this.secretPath = secretPath;
        return this;
    }

    /**
     * Returns the Vault polling interval (in seconds).
     *
     * @return the polling interval
     */
    public int getPollingInterval() {
        return pollingInterval;
    }

    /**
     * Sets the polling interval (in seconds) for checking for changes in Vault. The value 0
     * (default) disables polling.
     *
     * @param pollingInterval the polling interval
     * @return the updated {@link VaultSecureStoreConfig} instance
     * @throws IllegalArgumentException if pollingInterval is less than zero
     */
    public VaultSecureStoreConfig setPollingInterval(int pollingInterval) {
        checkNotNegative(pollingInterval, "Polling interval cannot be negative!");
        this.pollingInterval = pollingInterval;
        return this;
    }

    /**
     * Returns the SSL/TLS configuration.
     *
     * @return the SSL/TLS configuration
     */
    public SSLConfig getSSLConfig() {
        return sslConfig;
    }

    /**
     * Sets the SSL/TLS configuration.
     *
     * @param sslConfig the SSL/TLS configuration
     * @return the updated {@link VaultSecureStoreConfig} instance
     */
    public VaultSecureStoreConfig setSSLConfig(SSLConfig sslConfig) {
        this.sslConfig = sslConfig;
        return this;
    }

    @Override
    public String toString() {
        return "VaultSecureStoreConfig{"
                + "address='" + address + '\''
                + ", secretPath='" + secretPath + '\''
                + ", pollingInterval='" + pollingInterval + '\''
                + ", token='***'"
                + '}';
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VaultSecureStoreConfig)) {
            return false;
        }
        VaultSecureStoreConfig other = (VaultSecureStoreConfig) o;
        if (!Objects.equals(address, other.address)) {
            return false;
        }
        if (!Objects.equals(secretPath, other.secretPath)) {
            return false;
        }
        if (this.pollingInterval != other.pollingInterval) {
            return false;
        }
        return Objects.equals(token, other.token);
    }

    @Override
    public final int hashCode() {
        int result = address == null ? 0 : address.hashCode();
        result = 31 * result + (secretPath == null ? 0 : secretPath.hashCode());
        result = 31 * result + pollingInterval;
        result = 31 * result + (token == null ? 0 : token.hashCode());
        return result;
    }

}
