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

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Base class for symmetric encryption configuration classes.
 *
 * @param <T> the type of the configuration class
 */
public abstract class AbstractSymmetricEncryptionConfig<T extends AbstractSymmetricEncryptionConfig> {
    /**
     * Default symmetric encryption algorithm.
     */
    public static final String DEFAULT_SYMMETRIC_ALGORITHM = "AES/CBC/PKCS5Padding";

    /**
     * Default symmetric encryption salt.
     */
    public static final String DEFAULT_SYMMETRIC_SALT = "thesalt";

    boolean enabled;
    String algorithm = DEFAULT_SYMMETRIC_ALGORITHM;
    String salt = DEFAULT_SYMMETRIC_SALT;

    /**
     * Returns if this configuration is enabled.
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables and disables this configuration.
     *
     * @param enabled {@code true} to enable, {@code false} to disable
     */
    public T setEnabled(boolean enabled) {
        this.enabled = enabled;
        return (T) this;
    }

    /**
     * Returns the encryption algorithm.
     * @return the encryption algorithm
     */
    public String getAlgorithm() {
        return algorithm;
    }

    /**
     * Sets the encryption algorithm, such as {@code AES/CBC/PKCS5Padding}.
     *
     * @param algorithm the encryption algorithm
     */
    public T setAlgorithm(String algorithm) {
        checkNotNull(algorithm, "Encryption algorithm cannot be null!");
        this.algorithm = algorithm;
        return (T) this;
    }

    /**
     * Returns the salt.
     * @return the salt
     */
    public String getSalt() {
        return salt;
    }

    /**
     * Sets the salt used for encryption.
     *
     * @param salt the salt
     */
    public T setSalt(String salt) {
        this.salt = salt;
        return (T) this;
    }
}
