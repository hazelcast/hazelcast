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

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Contains configuration for the Hot Restart Persistence at Rest encryption
 */
public class EncryptionAtRestConfig extends AbstractSymmetricEncryptionConfig<EncryptionAtRestConfig> {

    private SecureStoreConfig secureStoreConfig = new NoSecureStoreConfig();

    private int keySize;

    /**
     * Returns the size (in bits) of the Hot Restart Store-level encryption key.
     *
     * @return the encryption key size in bits
     */
    public int getKeySize() {
        return keySize;
    }

    /**
     * Sets the size (in bits) of the Hot Restart Store-level encryption key
     * (non-positive value implies falling back to the cipher-specific default
     * key length).
     *
     * @param keySize the encryption key size in bits
     * @return the updated config instance
     */
    public EncryptionAtRestConfig setKeySize(int keySize) {
        this.keySize = keySize;
        return this;
    }

    /**
     * Returns the Secure Store configuration.
     *
     * @return the Secure Store configuration
     */
    public SecureStoreConfig getSecureStoreConfig() {
        return secureStoreConfig;
    }

    /**
     * Sets the Secure Store configuration.
     *
     * @param secureStoreConfig the Secure Store configuration
     * @return the updated config instance
     */
    public EncryptionAtRestConfig setSecureStoreConfig(SecureStoreConfig secureStoreConfig) {
        checkNotNull(secureStoreConfig, "Secure Store config cannot be null!");
        this.secureStoreConfig = secureStoreConfig;
        return this;
    }

    @Override
    public String toString() {
        return "EncryptionAtRestConfig{" + "enabled=" + enabled + ", algorithm='" + algorithm + '\'' + ", salt='***'"
                + ", keySize=" + keySize + ", secureStoreConfig=" + secureStoreConfig + '}';
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EncryptionAtRestConfig)) {
            return false;
        }
        EncryptionAtRestConfig other = (EncryptionAtRestConfig) o;
        if (enabled != other.enabled) {
            return false;
        }
        if (!Objects.equals(algorithm, other.algorithm)) {
            return false;
        }
        if (!Objects.equals(salt, other.salt)) {
            return false;
        }
        if (keySize != other.keySize) {
            return false;
        }
        return Objects.equals(secureStoreConfig, other.secureStoreConfig);
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (algorithm == null ? 0 : algorithm.hashCode());
        result = 31 * result + (salt == null ? 0 : salt.hashCode());
        result = 31 * result + keySize;
        result = 31 * result + (secureStoreConfig == null ? 0 : secureStoreConfig.hashCode());
        return result;
    }

    private static final class NoSecureStoreConfig extends SecureStoreConfig {
        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof NoSecureStoreConfig;
        }
    }

}

