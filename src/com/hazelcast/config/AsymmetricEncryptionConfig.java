/*
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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
    private String transformation = "RSA/NONE/PKCS1PADDING";
    private String keyPassword = "theKeyPass";
    private String keyAlias = "local";
    private String storeType = "JKS";
    private String storePassword = "theStorePass";
    private String storePath = "keystore";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getTransformation() {
        return transformation;
    }

    public void setTransformation(String transformation) {
        this.transformation = transformation;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public String getKeyAlias() {
        return keyAlias;
    }

    public void setKeyAlias(String keyAlias) {
        this.keyAlias = keyAlias;
    }

    public String getStoreType() {
        return storeType;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    public String getStorePassword() {
        return storePassword;
    }

    public void setStorePassword(String storePassword) {
        this.storePassword = storePassword;
    }

    public String getStorePath() {
        return storePath;
    }

    public void setStorePath(String storePath) {
        this.storePath = storePath;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("AsymmetricEncryptionConfig");
        sb.append("{enabled=").append(enabled);
        sb.append(", transformation='").append(transformation).append('\'');
        sb.append(", keyPassword='").append(keyPassword).append('\'');
        sb.append(", keyAlias='").append(keyAlias).append('\'');
        sb.append(", storeType='").append(storeType).append('\'');
        sb.append(", storePassword='").append(storePassword).append('\'');
        sb.append(", storePath='").append(storePath).append('\'');
        sb.append('}');
        return sb.toString();
    }
}