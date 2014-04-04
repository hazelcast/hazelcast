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

package com.hazelcast.transaction.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import javax.transaction.xa.Xid;
import java.io.IOException;
import java.util.Arrays;

public class SerializableXID implements Xid, DataSerializable {

    private int formatId;
    private byte[] globalTransactionId;
    private byte[] branchQualifier;

    public SerializableXID() {
    }

    public SerializableXID(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
        this.formatId = formatId;
        this.globalTransactionId = Arrays.copyOf(globalTransactionId, globalTransactionId.length);
        this.branchQualifier = Arrays.copyOf(branchQualifier, branchQualifier.length);
    }

    @Override
    public int getFormatId() {
        return formatId;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return Arrays.copyOf(globalTransactionId, globalTransactionId.length);
    }

    @Override
    public byte[] getBranchQualifier() {
        return Arrays.copyOf(branchQualifier, branchQualifier.length);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(formatId);
        out.writeInt(globalTransactionId.length);
        out.write(globalTransactionId);
        out.writeInt(branchQualifier.length);
        out.write(branchQualifier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        formatId = in.readInt();
        int gtiLen = in.readInt();
        globalTransactionId = new byte[gtiLen];
        in.readFully(globalTransactionId);
        int bqLen = in.readInt();
        branchQualifier = new byte[bqLen];
        in.readFully(branchQualifier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Xid)) {
            return false;
        }

        Xid that = (Xid) o;

        if (formatId != that.getFormatId()) {
            return false;
        }
        if (!Arrays.equals(branchQualifier, that.getBranchQualifier())) {
            return false;
        }
        if (!Arrays.equals(globalTransactionId, that.getGlobalTransactionId())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = formatId;
        result = 31 * result + Arrays.hashCode(globalTransactionId);
        result = 31 * result + Arrays.hashCode(branchQualifier);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SerializableXid{");
        sb.append("formatId=").append(formatId);
        sb.append(", globalTransactionId=").append(Arrays.toString(globalTransactionId));
        sb.append(", branchQualifier=").append(Arrays.toString(branchQualifier));
        sb.append('}');
        return sb.toString();
    }
}
