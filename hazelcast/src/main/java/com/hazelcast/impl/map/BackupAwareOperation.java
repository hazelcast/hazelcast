/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.map;

import com.hazelcast.impl.spi.AbstractNamedKeyBasedOperation;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.serialization.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class BackupAwareOperation extends AbstractNamedKeyBasedOperation {

    Data dataValue = null;
    long ttl = LockOperation.DEFAULT_LOCK_TTL; // how long should the lock live?
    long backupCallId = -1;
    String txnId = null;

    public BackupAwareOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public BackupAwareOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey);
        this.ttl = ttl;
    }

    public BackupAwareOperation(String name, Data dataKey, Data dataValue, long ttl) {
        super(name, dataKey);
        this.ttl = ttl;
        this.dataValue = dataValue;
    }

    public String getTxnId() {
        return txnId;
    }

    public void setTxnId(String txnId) {
        this.txnId = txnId;
    }

    public BackupAwareOperation() {
    }

    public void setBackupCallId(long backupCallId) {
        this.backupCallId = backupCallId;
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        SerializationHelper.writeNullableData(out, dataValue);
        out.writeLong(ttl);
        out.writeLong(backupCallId);
        boolean txnal = (txnId != null);
        out.writeBoolean(txnal);
        if (txnal) {
            out.writeUTF(txnId);
        }
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        dataValue = SerializationHelper.readNullableData(in);
        ttl = in.readLong();
        backupCallId = in.readLong();
        boolean txnal = in.readBoolean();
        if (txnal) {
            txnId = in.readUTF();
        }
    }
}