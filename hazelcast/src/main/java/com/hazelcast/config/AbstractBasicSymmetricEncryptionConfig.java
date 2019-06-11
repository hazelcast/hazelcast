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

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Base class for symmetric encryption configuration classes.
 *
 * @param <T> the type of the configuration class
 */
public abstract class AbstractBasicSymmetricEncryptionConfig<T extends AbstractBasicSymmetricEncryptionConfig> {
    /**
     * Default symmetric encryption algorithm.
     */
    public static final String DEFAULT_SYMMETRIC_ALGORITHM = "AES/CBC/PKCS5Padding";

    /**
     * Default symmetric encryption salt.
     */
    public static final String DEFAULT_SYMMETRIC_SALT = "thesalt";

    protected boolean enabled;
    protected String algorithm = DEFAULT_SYMMETRIC_ALGORITHM;
    protected String salt = DEFAULT_SYMMETRIC_SALT;

    public boolean isEnabled() {
        return enabled;
    }

    public T setEnabled(boolean enabled) {
        this.enabled = enabled;
        return (T) this;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public T setAlgorithm(String algorithm) {
        checkNotNull(algorithm, "Encryption algorithm cannot be null!");
        this.algorithm = algorithm;
        return (T) this;
    }

    public String getSalt() {
        return salt;
    }

    public T setSalt(String salt) {
        this.salt = salt;
        return (T) this;
    }
}
