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

import static java.util.Arrays.copyOf;

/**
 * Contains configuration for symmetric encryption
 */
public class SymmetricEncryptionConfig extends AbstractSymmetricEncryptionConfig<SymmetricEncryptionConfig> {

    private byte[] key;

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
                + "enabled=" + isEnabled()
                + ", algorithm='" + getAlgorithm() + '\''
                + ", password='***'"
                + ", salt='***'"
                + ", iterationCount=***"
                + ", key=***"
                + '}';
    }
}
