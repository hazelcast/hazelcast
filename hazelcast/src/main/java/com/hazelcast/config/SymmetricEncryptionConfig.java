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

public class SymmetricEncryptionConfig {
    private boolean enabled = false;
    private String salt = "thesalt";
    private String password = "thepassword";
    private int iterationCount = 19;
    private String algorithm = "PBEWithMD5AndDES";
    private byte[] key = null;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getSalt() {
        return salt;
    }

    public void setSalt(String salt) {
        this.salt = salt;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getIterationCount() {
        return iterationCount;
    }

    public void setIterationCount(int iterationCount) {
        this.iterationCount = iterationCount;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("SymmetricEncryptionConfig");
        sb.append("{enabled=").append(enabled);
        sb.append(", salt='").append(salt).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", iterationCount=").append(iterationCount);
        sb.append(", algorithm='").append(algorithm).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
