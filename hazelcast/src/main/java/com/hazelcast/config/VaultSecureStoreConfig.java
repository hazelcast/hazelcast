/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * HashiCorp Vault Secure Store configuration.
 * <p>
 * The Vault Secure Store uses the Vault REST API to communicate with Vault. The Vault Secure Store
 * expects the encryption keys to be stored as key/value pairs under the same secret path. For each
 * key/value pair, the key represents the name of the encryption key and the value a Base64-encoded
 * value of the key. For instance, the following invocation of the Vault command-line client:
 * <pre>
 * vault kv put hz/hotrestart/cluster1 current=@current.base64 prev=@prev.base64
 * </pre>
 * results in two keys (named "current" and "prev") to be stored under the secret path
 * "hz/hotrestart/cluster1".
 * <p>
 * The Vault Secure Store can communicate with the Vault server using HTTP or HTTPS. For the latter,
 * appropriate SSL/TLS configuration must be provided.
 */
public class VaultSecureStoreConfig extends SecureStoreConfig {

    /**
     * The HashiCorp Vault secret engine version.
     */
    public enum SecretEngineVersion {
        /**
         * Version 1.
         */
        V1,
        /**
         * Version 2.
         */
        V2
    }

    /**
     * The default secret engine version to use ({@link SecretEngineVersion#V2}).
     */
    public static final SecretEngineVersion DEFAULT_SECRET_ENGINE_VERSION = SecretEngineVersion.V2;

    private String address;
    private String secretPath;
    private SecretEngineVersion secretEngineVersion = DEFAULT_SECRET_ENGINE_VERSION;
    private String token;
    private String namespace;
    private SSLConfig sslConfig;
    private List<Entry> entries = new ArrayList<>();

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
     * @return the Vault secret path
     */
    public String getSecretPath() {
        return secretPath;
    }

    /**
     * Sets the Vault secret path.
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
     * Returns the Vault secret engine version.
     *
     * @return the Vault secret engine version
     */
    public SecretEngineVersion getSecretEngineVersion() {
        return secretEngineVersion;
    }

    /**
     * Sets the Vault secret engine version.
     *
     * @param secretEngineVersion the secret engine version
     * @return the updated {@link VaultSecureStoreConfig} instance
     * @throws IllegalArgumentException if secretEngineVersion is {code null}
     */
    public VaultSecureStoreConfig setSecretEngineVersion(SecretEngineVersion secretEngineVersion) {
        checkNotNull(secretEngineVersion, "Vault secret engine version cannot be null!");
        this.secretEngineVersion = secretEngineVersion;
        return this;
    }

    /**
     * Returns the Vault namespace.
     *
     * @return the Vault namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Sets the Vault namespace.
     *
     * @param namespace the namespace
     * @return the updated {@link VaultSecureStoreConfig} instance
     */
    public VaultSecureStoreConfig setNamespace(String namespace) {
        this.namespace = namespace;
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

    /**
     * Returns the key entries to be looked up in Vault
     *
     * @return a list of {@link Entry} objects.
     */
    public List<Entry> getEntries() {
        return entries;
    }

    /**
     * Sets the entries to be looked in Vault under the provided secret path.
     *
     * @param entries a list of {@link Entry} objects
     * @return the updated {@link VaultSecureStoreConfig} instance
     * @throws IllegalArgumentException if entries is {code null}
     */
    public VaultSecureStoreConfig setEntries(List<Entry> entries) {
        checkNotNull(entries, "Vault entries cannot be null!");
        this.entries = entries;
        return this;
    }

    @Override
    public String toString() {
        return "VaultSecureStoreConfig{" + "address='" + address + '\'' + ", secretPath='" + secretPath + '\''
                + ", secretEngineVersion=" + secretEngineVersion + ", entries=" + entries + ", token='***'" + ", namespace=" + (
                namespace == null ? null : ('\'' + namespace + '\'')) + '}';
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
        if (secretEngineVersion != other.secretEngineVersion) {
            return false;
        }
        if (!Objects.equals(token, other.token)) {
            return false;
        }
        if (!Objects.equals(entries, other.entries)) {
            return false;
        }
        return Objects.equals(namespace, other.namespace);
    }

    @Override
    public final int hashCode() {
        int result = address == null ? 0 : address.hashCode();
        result = 31 * result + (secretPath == null ? 0 : secretPath.hashCode());
        result = 31 * result + (secretEngineVersion == null ? 0 : secretEngineVersion.hashCode());
        result = 31 * result + (token == null ? 0 : token.hashCode());
        result = 31 * result + (entries == null ? 0 : entries.hashCode());
        result = 31 * result + (namespace == null ? 0 : namespace.hashCode());
        return result;
    }

    /**
     * An entry to be looked up under the provided secret path.
     */
    public static class Entry {
        private final String name;

        /**
         * Constructs a new entry with given name.
         *
         * @param name the entry name
         */
        public Entry(String name) {
            this.name = name;
        }

        /**
         * Returns the entry name.
         *
         * @return the entry name
         */
        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "Entry{" + "name='" + name + '\'' + '}';
        }

        @Override
        public final boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Entry)) {
                return false;
            }
            Entry other = (Entry) o;
            return Objects.equals(name, other.name);
        }

        @Override
        public final int hashCode() {
            return name == null ? 0 : name.hashCode();
        }
    }
}
