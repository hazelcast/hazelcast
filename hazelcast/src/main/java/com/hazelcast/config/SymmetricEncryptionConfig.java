/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Arrays;

public class SymmetricEncryptionConfig {

    private static final int ITERATION_COUNT = 19;

    private boolean enabled;
    private String salt = "thesalt";
    private String password = "thepassword";
    private int iterationCount = ITERATION_COUNT;
    private String algorithm = "PBEWithMD5AndDES";
    private byte[] key;

    public boolean isEnabled() {
        return enabled;
    }

    public SymmetricEncryptionConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getSalt() {
        return salt;
    }

    public SymmetricEncryptionConfig setSalt(String salt) {
        this.salt = salt;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public SymmetricEncryptionConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public int getIterationCount() {
        return iterationCount;
    }

    public SymmetricEncryptionConfig setIterationCount(int iterationCount) {
        this.iterationCount = iterationCount;
        return this;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public SymmetricEncryptionConfig setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    public byte[] getKey() {
        return key != null ? Arrays.copyOf(key, key.length) : null;
    }

    public SymmetricEncryptionConfig setKey(byte[] key) {
        this.key = key != null ? Arrays.copyOf(key, key.length) : null;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SymmetricEncryptionConfig{");
        sb.append("enabled=").append(enabled);
        sb.append(", iterationCount=").append(iterationCount);
        sb.append(", algorithm='").append(algorithm).append('\'');
        sb.append(", key=").append(Arrays.toString(key));
        sb.append('}');
        return sb.toString();
    }

}
