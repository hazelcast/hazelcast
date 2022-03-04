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

import java.io.File;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Java KeyStore Secure Store configuration.
 * <p>
 * The Java KeyStore Secure Store exposes (symmetric) encryption keys stored in a Java KeyStore
 * with path, type, and password as specified by {@link #getType()}, {@link #getPath()}, and
 * {@link #getPassword()}, respectively. The Java KeyStore SecureStore loads all symmetric keys
 * available in the KeyStore and treats them as versions of a single key. More specifically:
 * <ul>
 *     <li>If an alias for the current encryption key is set (see
 *     {@link #setCurrentKeyAlias(String)}), the corresponding KeyStore entry is treated as
 *     the current version of the encryption key, while any other entries are treated as
 *     historical versions of the encryption key.</li>
 *     <li>If an alias for the current encryption key is not set, the KeyStore entry with an
 *     alias that comes last in alphabetical order is treated as the current version of the
 *     encryption key, while any other entries are treated as historical versions of the
 *     encryption key.</li>
 *     <li>The KeyStore entries are expected to use the same password as the KeyStore.</li>
 * </ul>
 * The Java KeyStore can also poll the KeyStore for changes at preconfigured interval (disabled
 * by default).
 * <p>
 * Only file-based KeyStores are supported.
 */
public class JavaKeyStoreSecureStoreConfig
        extends SecureStoreConfig {
    /**
     * The default Java KeyStore type (PKCS12).
     */
    public static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";

    /**
     * Default interval (in seconds) for polling for changes in the KeyStore: 0 (polling
     * disabled).
     */
    public static final int DEFAULT_POLLING_INTERVAL = 0;

    private File path;
    private String type = DEFAULT_KEYSTORE_TYPE;
    private String password;
    private String currentKeyAlias;
    private int pollingInterval = DEFAULT_POLLING_INTERVAL;

    /**
     * Creates a new Java KeyStore Secure Store configuration.
     */
    public JavaKeyStoreSecureStoreConfig() {
    }

    /**
     * Creates a new Java KeyStore Secure Store configuration.
     * @param path the KeyStore file path
     */
    public JavaKeyStoreSecureStoreConfig(File path) {
        checkNotNull(path, "Java Key Store path cannot be null!");
        this.path = path;
    }

    /**
     * Returns the type of the Java KeyStore.
     * @return the Java KeyStore type
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the Java KeyStore type (PKCS12, JCEKS etc.)
     * @param type the KeyStore type
     * @return the updated {@link JavaKeyStoreSecureStoreConfig} instance
     * @throws IllegalArgumentException if type is {code null}
     */
    public JavaKeyStoreSecureStoreConfig setType(String type) {
        checkNotNull(type, "Java Key Store type cannot be null!");
        this.type = type;
        return this;
    }

    /**
     * Returns the Java KeyStore file path.
     * @return the file path
     */
    public File getPath() {
        return path;
    }

    /**
     * Sets the Java KeyStore file path.
     * @param path the file path
     * @return the updated {@link JavaKeyStoreSecureStoreConfig} instance
     * @throws IllegalArgumentException if path is {code null}
     */
    public JavaKeyStoreSecureStoreConfig setPath(File path) {
        checkNotNull(path, "Java Key Store path cannot be null!");
        this.path = path;
        return this;
    }

    /**
     * Returns the Java KeyStore password.
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the Java KeyStore password.
     * @param password the KeyStore password
     * @return the updated {@link JavaKeyStoreSecureStoreConfig} instance
     */
    public JavaKeyStoreSecureStoreConfig setPassword(String password) {
        // null password allowed
        this.password = password;
        return this;
    }

    /**
     * Returns the alias for the current encryption key entry or {@code null} if no alias is set.
     * @return the alias or {@code null}
     */
    public String getCurrentKeyAlias() {
        return currentKeyAlias;
    }

    /**
     * Sets the alias for the current encryption key entry.
     * @param currentKeyAlias the alias for the current encryption key or {@code null}
     * @return the updated {@link JavaKeyStoreSecureStoreConfig} instance
     */
    public JavaKeyStoreSecureStoreConfig setCurrentKeyAlias(String currentKeyAlias) {
        this.currentKeyAlias = currentKeyAlias;
        return this;
    }

    /**
     * Returns the polling interval (in seconds) for checking for changes in the KeyStore.
     *
     * @return the polling interval
     */
    public int getPollingInterval() {
        return pollingInterval;
    }

    /**
     * Sets the polling interval (in seconds) for checking for changes in the KeyStore. The value 0 disables polling.
     *
     * @param pollingInterval the polling interval
     * @return the updated {@link JavaKeyStoreSecureStoreConfig} instance
     * @throws IllegalArgumentException if pollingInterval is less than zero
     */
    public JavaKeyStoreSecureStoreConfig setPollingInterval(int pollingInterval) {
        checkNotNegative(pollingInterval, "Polling interval cannot be negative!");
        this.pollingInterval = pollingInterval;
        return this;
    }

    @Override
    public String toString() {
        return "JavaKeyStoreSecureStoreConfig{"
                + "path='" + path + '\''
                + ", type='" + type + '\''
                + ", pollingInterval='" + pollingInterval + '\''
                + ", currentKeyAlias='" + currentKeyAlias + '\''
                + ", password='***'"
                + '}';
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JavaKeyStoreSecureStoreConfig)) {
            return false;
        }
        JavaKeyStoreSecureStoreConfig other = (JavaKeyStoreSecureStoreConfig) o;
        if (!Objects.equals(path, other.path)) {
            return false;
        }
        if (!Objects.equals(type, other.type)) {
            return false;
        }
        if (!Objects.equals(password, other.password)) {
            return false;
        }
        if (!Objects.equals(currentKeyAlias, other.currentKeyAlias)) {
            return false;
        }
        return this.pollingInterval == other.pollingInterval;
    }

    @Override
    public final int hashCode() {
        int result = path == null ? 0 : path.hashCode();
        result = 31 * result + (type == null ? 0 : type.hashCode());
        result = 31 * result + (password == null ? 0 : password.hashCode());
        result = 31 * result + (currentKeyAlias == null ? 0 : currentKeyAlias.hashCode());
        result = 31 * result + pollingInterval;
        return result;
    }

}
