/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutAllBackupOperation extends MapOperation
        implements PartitionAwareOperation, BackupOperation, Versioned {

    private boolean disableWanReplicationEvent;
    private List keyValueRecordExpiryWan;

    private transient int lastIndex;
    private transient List keyRecordExpiryWan;

    public PutAllBackupOperation(String name,
                                 List keyValueRecordExpiryWan,
                                 boolean disableWanReplicationEvent) {
        super(name);
        this.keyValueRecordExpiryWan = keyValueRecordExpiryWan;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    public PutAllBackupOperation() {
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    protected void runInternal() {
        List keyRecordExpiryWan = this.keyRecordExpiryWan;
        if (keyRecordExpiryWan != null) {
            for (int i = lastIndex; i < keyRecordExpiryWan.size(); i += 3) {
                Data key = (Data) keyRecordExpiryWan.get(i);
                Record record = (Record) keyRecordExpiryWan.get(i + 1);
                ExpiryMetadata expiryMetadata = (ExpiryMetadata) keyRecordExpiryWan.get(i + 2);
                boolean shouldWanReplicate = (Boolean) keyRecordExpiryWan.get(i + 3);
                putBackup(key, record, expiryMetadata, shouldWanReplicate);
                lastIndex = i;
            }
        } else {
            // If dataKeyRecord is null and we are in
            // `runInternal` method, means this operation
            // has not been serialized/deserialized
            // and is running directly on caller node
            List keyValueRecordExpiry = this.keyValueRecordExpiryWan;
            for (int i = lastIndex; i < keyValueRecordExpiry.size(); i += 5) {
                Data key = (Data) keyValueRecordExpiry.get(i);
                Record record = (Record) keyValueRecordExpiry.get(i + 2);
                ExpiryMetadata expiryMetadata = (ExpiryMetadata) keyValueRecordExpiry.get(i + 3);
                boolean shouldWanReplicate = (Boolean) keyValueRecordExpiry.get(i + 4);
                putBackup(key, record, expiryMetadata, shouldWanReplicate);
                lastIndex = i;
            }
        }
    }

    private void putBackup(Data key, Record record, ExpiryMetadata expiryMetadata, boolean shouldWanReplicate) {
        Record currentRecord = recordStore.putBackup(key, record,
                expiryMetadata.getTtl(), expiryMetadata.getMaxIdle(),
                expiryMetadata.getExpirationTime(),
                getCallerProvenance());
        Records.copyMetadataFrom(record, currentRecord);
        if (shouldWanReplicate) {
            publishWanUpdate(key, record.getValue());
        }
        evict(key);
    }

    @Override
    protected boolean disableWanReplicationEvent() {
        return disableWanReplicationEvent;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeInt(keyValueRecordExpiryWan.size() / 4);
        for (int i = 0; i < keyValueRecordExpiryWan.size(); i += 4) {
            Data dataKey = (Data) keyValueRecordExpiryWan.get(i);
            Data dataValue = (Data) keyValueRecordExpiryWan.get(i + 1);
            Record record = (Record) keyValueRecordExpiryWan.get(i + 2);
            ExpiryMetadata expiryMetadata = (ExpiryMetadata) keyValueRecordExpiryWan.get(i + 3);
            Boolean shouldWanReplicate = (Boolean) keyValueRecordExpiryWan.get(i + 4);

            IOUtil.writeData(out, dataKey);
            Records.writeRecord(out, record, dataValue);
            Records.writeExpiry(out, expiryMetadata);
            if (out.getVersion().isGreaterOrEqual(Versions.V5_4)) { // TODO: Can't release on 5.3?
                out.writeBoolean(shouldWanReplicate);
            }
        }
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        int size = in.readInt();
        List keyRecordExpiryWan = new ArrayList<>(size * 3);
        for (int i = 0; i < size; i++) {
            Data dataKey = IOUtil.readData(in);
            Record record = Records.readRecord(in);
            ExpiryMetadata expiryMetadata = Records.readExpiry(in);
            boolean shouldWanReplicate = true;
            if (in.getVersion().isGreaterOrEqual(Versions.V5_4)) { // TODO: Can't release on 5.3?
                shouldWanReplicate = in.readBoolean();
            }

            keyRecordExpiryWan.add(dataKey);
            keyRecordExpiryWan.add(record);
            keyRecordExpiryWan.add(expiryMetadata);
            keyRecordExpiryWan.add(shouldWanReplicate);
        }
        this.keyRecordExpiryWan = keyRecordExpiryWan;
        this.disableWanReplicationEvent = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_ALL_BACKUP;
    }
}
