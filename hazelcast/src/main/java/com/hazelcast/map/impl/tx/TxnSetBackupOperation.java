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

package com.hazelcast.map.impl.tx;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.PutBackupOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.map.impl.record.Records.applyRecordInfo;

public class TxnSetBackupOperation extends PutBackupOperation {
    private UUID transactionId;

    public TxnSetBackupOperation() {
    }

    public TxnSetBackupOperation(String name, Data dataKey, Data dataValue,
                                 RecordInfo recordInfo, UUID transactionId) {
        super(name, dataKey, dataValue, recordInfo);
        this.transactionId = transactionId;
    }

    @Override
    protected void runInternal() {
        Record record = recordStore.putBackupTxn(dataKey, dataValue, recordInfo.getTtl(),
                recordInfo.getMaxIdle(), isPutTransient(), getCallerProvenance(), transactionId);

        applyRecordInfo(record, recordInfo);
        recordStore.forceUnlock(dataKey);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionId = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TXN_SET_BACKUP;
    }
}
