/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.ExpiryMetadata;
import com.hazelcast.map.impl.recordstore.ExpirySystem;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class PutWithExpiryBackupOperation
        extends PutBackupOperation {

    public PutWithExpiryBackupOperation(String name, Data dataKey,
                                        Record<Data> record,
                                        Data dataValue, ExpiryMetadata expiryMetadata) {
        super(name, dataKey, record, dataValue, expiryMetadata);
    }

    public PutWithExpiryBackupOperation() {
    }

    @Override
    protected void runInternal() {
        // TODO performance: we can put this record directly
        // into record-store if memory format is BINARY
        Record currentRecord = recordStore.putBackup(dataKey,
                // TODO also put expiryTime from expiryMetadata
                record, expiryMetadata.getTtl(), expiryMetadata.getMaxIdle(), getCallerProvenance());
        Records.copyMetadataFrom(record, currentRecord);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_WITH_EXPIRY_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        IOUtil.writeData(out, dataKey);
        Records.writeRecord(out, record, dataValue, expiryMetadata);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        dataKey = IOUtil.readData(in);
        expiryMetadata = new ExpirySystem.ExpiryMetadataImpl();
        record = Records.readRecord(in, expiryMetadata);
    }
}
