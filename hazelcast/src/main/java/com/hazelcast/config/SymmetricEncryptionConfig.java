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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.ByteUtil;

import java.io.IOException;

public class SymmetricEncryptionConfig implements DataSerializable {
    private boolean enabled = false;
    private String salt = "thesalt";
    private String password = "thepassword";
    private int iterationCount = 19;
    private String algorithm = "PBEWithMD5AndDES";
    private byte[] key = null;

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
        return key;
    }

    public SymmetricEncryptionConfig setKey(byte[] key) {
        this.key = key;
        return this;
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

    public void writeData(ObjectDataOutput out) throws IOException {
        boolean hasKey = key != null && key.length > 0;
        out.writeByte(ByteUtil.toByte(enabled, hasKey));
        if (enabled) {
            out.writeUTF(salt);
            out.writeUTF(password);
            out.writeInt(iterationCount);
            out.writeUTF(algorithm);
            if (hasKey) {
                out.writeInt(key.length);
                out.write(key);
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        boolean[] b = ByteUtil.fromByte(in.readByte());
        enabled = b[0];
        if (enabled) {
            salt = in.readUTF();
            password = in.readUTF();
            iterationCount = in.readInt();
            algorithm = in.readUTF();
            boolean hasKey = b[1];
            if (hasKey) {
                key = new byte[in.readInt()];
                in.readFully(key);
            }
        }
    }
}
