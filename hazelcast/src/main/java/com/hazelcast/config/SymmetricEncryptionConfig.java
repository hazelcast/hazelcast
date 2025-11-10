/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
 * @deprecated
 */
@Deprecated(since = "4.2")
public class SymmetricEncryptionConfig
        extends AbstractSymmetricEncryptionConfig<SymmetricEncryptionConfig> {

    /**
     * Default symmetric encryption algorithm
     */
    public static final String DEFAULT_SYMMETRIC_ALGORITHM = "PBEWithMD5AndDES";

    /**
     * Default symmetric encryption password used in Hazelcast before version 5.5.
     */
    public static final String DEFAULT_SYMMETRIC_PASSWORD = "thepassword";

    private static final int DEFAULT_ITERATION_COUNT = 19;

    private int iterationCount = DEFAULT_ITERATION_COUNT;
    private String password;
    private byte[] key;

    public SymmetricEncryptionConfig() {
        algorithm = DEFAULT_SYMMETRIC_ALGORITHM;
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
