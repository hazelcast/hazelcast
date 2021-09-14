/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadataImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.version.Version;

import java.io.IOException;

public class PutBackupOperation
        extends MapOperation implements BackupOperation, Versioned {

    protected Record<Data> record;
    protected Data dataKey;
    protected Data dataValue;
    protected ExpiryMetadata expiryMetadata;

    public PutBackupOperation(String name, Data dataKey,
                              Record<Data> record, Data dataValue,
                              ExpiryMetadata expiryMetadata) {
        super(name);
        this.dataKey = dataKey;
        this.record = record;
        this.dataValue = dataValue;
        this.expiryMetadata = expiryMetadata;
    }

    public PutBackupOperation() {
    }

    @Override
    protected void runInternal() {
        // TODO performance: we can put this record directly into record-store if memory format is BINARY
        Record currentRecord = recordStore.putBackup(dataKey, record,
                expiryMetadata, isPutTransient(), getCallerProvenance());
        Records.copyMetadataFrom(record, currentRecord);
    }

    protected boolean isPutTransient() {
        return false;
    }

    @Override
    protected void afterRunInternal() {
        evict(dataKey);
        publishWanUpdate(dataKey, record.getValue());

        super.afterRunInternal();
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        IOUtil.writeData(out, dataKey);
        Records.writeRecord(out, record, dataValue, expiryMetadata);
        // RU_COMPAT_4_2
        Version version = out.getVersion();
        if (version.isGreaterOrEqual(Versions.V5_0)) {
            Records.writeExpiry(out, expiryMetadata);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        // RU_COMPAT_4_2
        Version version = in.getVersion();
        boolean isV5 = version.isGreaterOrEqual(Versions.V5_0);

        dataKey = IOUtil.readData(in);
        if (!isV5) {
            expiryMetadata = new ExpiryMetadataImpl();
        }
        record = Records.readRecord(in, expiryMetadata);
        if (isV5) {
            expiryMetadata = Records.readExpiry(in);
        }
    }
}
