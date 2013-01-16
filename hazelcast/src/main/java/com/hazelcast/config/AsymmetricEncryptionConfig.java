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

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AsymmetricEncryptionConfig implements DataSerializable {
    private boolean enabled = false;
    private String algorithm = "RSA/NONE/PKCS1PADDING";
    private String keyPassword = "thekeypass";
    private String keyAlias = "local";
    private String storeType = "JKS";
    private String storePassword = "thestorepass";
    private String storePath = "keystore";

    public boolean isEnabled() {
        return enabled;
    }

    public AsymmetricEncryptionConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public AsymmetricEncryptionConfig setAlgorithm(final String algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public AsymmetricEncryptionConfig setKeyPassword(final String keyPassword) {
        this.keyPassword = keyPassword;
        return this;
    }

    public String getKeyAlias() {
        return keyAlias;
    }

    public AsymmetricEncryptionConfig setKeyAlias(final String keyAlias) {
        this.keyAlias = keyAlias;
        return this;
    }

    public String getStoreType() {
        return storeType;
    }

    public AsymmetricEncryptionConfig setStoreType(final String storeType) {
        this.storeType = storeType;
        return this;
    }

    public String getStorePassword() {
        return storePassword;
    }

    public AsymmetricEncryptionConfig setStorePassword(final String storePassword) {
        this.storePassword = storePassword;
        return this;
    }

    public String getStorePath() {
        return storePath;
    }

    public AsymmetricEncryptionConfig setStorePath(final String storePath) {
        this.storePath = storePath;
        return this;
    }

    @Override
    public String toString() {
        return new StringBuilder(256)
                .append("AsymmetricEncryptionConfig")
                .append("{enabled=").append(enabled)
                .append(", algorithm='").append(algorithm).append('\'')
                .append(", keyPassword='").append(keyPassword).append('\'')
                .append(", keyAlias='").append(keyAlias).append('\'')
                .append(", storeType='").append(storeType).append('\'')
                .append(", storePassword='").append(storePassword).append('\'')
                .append(", storePath='").append(storePath).append('\'')
                .append('}')
                .toString();
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeBoolean(enabled);
        if (enabled) {
            out.writeUTF(algorithm);
            out.writeUTF(keyPassword);
            out.writeUTF(keyAlias);
            out.writeUTF(storeType);
            out.writeUTF(storePassword);
            out.writeUTF(storePath);
        }
    }

    public void readData(DataInput in) throws IOException {
        enabled = in.readBoolean();
        if (enabled) {
            algorithm = in.readUTF();
            keyPassword = in.readUTF();
            keyAlias = in.readUTF();
            storeType = in.readUTF();
            storePassword = in.readUTF();
            storePath = in.readUTF();
        }
    }
}
