/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static java.util.Arrays.copyOf;

/**
 * Contains configuration for symmetric encryption
 */
public class SymmetricEncryptionConfig {

    /**
     * Default symmetric encryption password
     */
    public static final String DEFAULT_SYMMETRIC_PASSWORD = "thepassword";

    /**
     * Default symmetric encryption salt
     */
    public static final String DEFAULT_SYMMETRIC_SALT = "thesalt";

    private static final int ITERATION_COUNT = 19;

    private boolean enabled;
    private String algorithm = "PBEWithMD5AndDES";
    private String password = DEFAULT_SYMMETRIC_PASSWORD;
    private String salt = DEFAULT_SYMMETRIC_SALT;
    private int iterationCount = ITERATION_COUNT;
    private byte[] key;

    public boolean isEnabled() {
        return enabled;
    }

    public SymmetricEncryptionConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public SymmetricEncryptionConfig setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public SymmetricEncryptionConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getSalt() {
        return salt;
    }

    public SymmetricEncryptionConfig setSalt(String salt) {
        this.salt = salt;
        return this;
    }

    public int getIterationCount() {
        return iterationCount;
    }

    public SymmetricEncryptionConfig setIterationCount(int iterationCount) {
        this.iterationCount = iterationCount;
        return this;
    }

    public byte[] getKey() {
        return cloneKey(key);
    }

    public SymmetricEncryptionConfig setKey(byte[] key) {
        this.key = cloneKey(key);
        return this;
    }

    private byte[] cloneKey(byte[] key) {
        return key != null ? copyOf(key, key.length) : null;
    }

    @Override
    public String toString() {
        return "SymmetricEncryptionConfig{"
                + "enabled=" + enabled
                + ", algorithm='" + algorithm + '\''
                + ", password='***'"
                + ", salt='***'"
                + ", iterationCount=***"
                + ", key=***"
                + '}';
    }
}
