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

package com.hazelcast.map;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.AbstractNamedKeyBasedOperation;

import java.io.IOException;

public abstract class TTLAwareOperation extends AbstractNamedKeyBasedOperation {

    Data dataValue = null;
    long ttl = -1; // how long should this item live? -1 means forever
    String txnId = null;

    public TTLAwareOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public TTLAwareOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey);
        this.ttl = ttl;
    }

    public TTLAwareOperation(String name, Data dataKey, Data dataValue, long ttl) {
        super(name, dataKey);
        this.ttl = ttl;
        this.dataValue = dataValue;
    }

    public Data getValue() {
        return dataValue;
    }

    public long getTtl() {
        return ttl;
    }

    public String getTxnId() {
        return txnId;
    }

    public void setTxnId(String txnId) {
        this.txnId = txnId;
    }

    public TTLAwareOperation() {
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeNullableData(out, dataValue);
        out.writeLong(ttl);
        boolean txnal = (txnId != null);
        out.writeBoolean(txnal);
        if (txnal) {
            out.writeUTF(txnId);
        }
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dataValue = IOUtil.readNullableData(in);
        ttl = in.readLong();
        boolean txnal = in.readBoolean();
        if (txnal) {
            txnId = in.readUTF();
        }
    }
}