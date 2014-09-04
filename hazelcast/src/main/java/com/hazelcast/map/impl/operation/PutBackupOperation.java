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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

public final class PutBackupOperation extends KeyBasedMapOperation implements BackupOperation, IdentifiedDataSerializable {

    // todo unlockKey is a logic just used in transactional put operations.
    // todo It complicates here there should be another Operation for that logic. e.g. TxnSetBackup
    private boolean unlockKey;
    private RecordInfo recordInfo;

    public PutBackupOperation(String name, Data dataKey, Data dataValue, RecordInfo recordInfo) {
        super(name, dataKey, dataValue);
        this.recordInfo = recordInfo;
    }

    public PutBackupOperation(String name, Data dataKey, Data dataValue, RecordInfo recordInfo, boolean unlockKey) {
        super(name, dataKey, dataValue);
        this.unlockKey = unlockKey;
        this.recordInfo = recordInfo;
    }

    public PutBackupOperation() {
    }

    public void run() {
        final Record record = recordStore.putBackup(dataKey, dataValue, ttl);
        if (recordInfo != null) {
            Records.applyRecordInfo(record, recordInfo);
        }
        if (unlockKey) {
            recordStore.forceUnlock(dataKey);
        }
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(unlockKey);
        if (recordInfo != null) {
            out.writeBoolean(true);
            recordInfo.writeData(out);
        } else {
            out.writeBoolean(false);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        unlockKey = in.readBoolean();
        boolean hasRecordInfo = in.readBoolean();
        if (hasRecordInfo) {
            recordInfo = new RecordInfo();
            recordInfo.readData(in);
        }
    }

    @Override
    public String toString() {
        return "PutBackupOperation{" + name + "}";
    }

    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    public int getId() {
        return MapDataSerializerHook.PUT_BACKUP;
    }


}
