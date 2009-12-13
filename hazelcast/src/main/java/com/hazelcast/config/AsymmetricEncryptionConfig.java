/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

public class AsymmetricEncryptionConfig {
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

    public AsymmetricEncryptionConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public AsymmetricEncryptionConfig setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public AsymmetricEncryptionConfig setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
        return this;
    }

    public String getKeyAlias() {
        return keyAlias;
    }

    public AsymmetricEncryptionConfig setKeyAlias(String keyAlias) {
        this.keyAlias = keyAlias;
        return this;
    }

    public String getStoreType() {
        return storeType;
    }

    public AsymmetricEncryptionConfig setStoreType(String storeType) {
        this.storeType = storeType;
        return this;
    }

    public String getStorePassword() {
        return storePassword;
    }

    public AsymmetricEncryptionConfig setStorePassword(String storePassword) {
        this.storePassword = storePassword;
        return this;
    }

    public String getStorePath() {
        return storePath;
    }

    public AsymmetricEncryptionConfig setStorePath(String storePath) {
        this.storePath = storePath;
        return this;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("AsymmetricEncryptionConfig");
        sb.append("{enabled=").append(enabled);
        sb.append(", algorithm='").append(algorithm).append('\'');
        sb.append(", keyPassword='").append(keyPassword).append('\'');
        sb.append(", keyAlias='").append(keyAlias).append('\'');
        sb.append(", storeType='").append(storeType).append('\'');
        sb.append(", storePassword='").append(storePassword).append('\'');
        sb.append(", storePath='").append(storePath).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
