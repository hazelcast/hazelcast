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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static com.hazelcast.internal.cluster.Versions.V5_4;

public class PutAllBackupOperation extends MapOperation
        implements PartitionAwareOperation, BackupOperation, Versioned {

    private boolean disableWanReplicationEvent;
    private List keyValueRecordExpiry;
    @Nullable
    private BitSet noWanReplicationKeys;

    private transient int lastIndex;
    private transient List keyRecordExpiry;

    public PutAllBackupOperation(String name,
                                 List keyValueRecordExpiry,
                                 @Nullable BitSet noWanReplicationKeys,
                                 boolean disableWanReplicationEvent) {
        super(name);
        this.keyValueRecordExpiry = keyValueRecordExpiry;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
        this.noWanReplicationKeys = noWanReplicationKeys;
    }

    public PutAllBackupOperation(String name,
                                 List keyValueRecordExpiry,
                                 boolean disableWanReplicationEvent) {
        this(name, keyValueRecordExpiry, null, disableWanReplicationEvent);
    }

    public PutAllBackupOperation() {
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    protected void runInternal() {
        List keyRecordExpiry = this.keyRecordExpiry;
        if (keyRecordExpiry != null) {
            for (int i = lastIndex; i < keyRecordExpiry.size(); i += 3) {
                Data key = (Data) keyRecordExpiry.get(i);
                Record record = (Record) keyRecordExpiry.get(i + 1);
                ExpiryMetadata expiryMetadata = (ExpiryMetadata) keyRecordExpiry.get(i + 2);
                boolean wanReplicated = noWanReplicationKeys == null || !noWanReplicationKeys.get(i / 3);
                putBackup(key, record, expiryMetadata, wanReplicated);
                lastIndex = i;
            }
        } else {
            // If dataKeyRecord is null and we are in
            // `runInternal` method, means this operation
            // has not been serialized/deserialized
            // and is running directly on caller node
            List keyValueRecordExpiry = this.keyValueRecordExpiry;
            for (int i = lastIndex; i < keyValueRecordExpiry.size(); i += 4) {
                Data key = (Data) keyValueRecordExpiry.get(i);
                Record record = (Record) keyValueRecordExpiry.get(i + 2);
                ExpiryMetadata expiryMetadata = (ExpiryMetadata) keyValueRecordExpiry.get(i + 3);
                boolean wanReplicated = noWanReplicationKeys == null || !noWanReplicationKeys.get(i / 4);
                putBackup(key, record, expiryMetadata, wanReplicated);
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

        out.writeInt(keyValueRecordExpiry.size() / 4);
        for (int i = 0; i < keyValueRecordExpiry.size(); i += 4) {
            Data dataKey = (Data) keyValueRecordExpiry.get(i);
            Data dataValue = (Data) keyValueRecordExpiry.get(i + 1);
            Record record = (Record) keyValueRecordExpiry.get(i + 2);
            ExpiryMetadata expiryMetadata = (ExpiryMetadata) keyValueRecordExpiry.get(i + 3);

            IOUtil.writeData(out, dataKey);
            Records.writeRecord(out, record, dataValue);
            Records.writeExpiry(out, expiryMetadata);
        }
        out.writeBoolean(disableWanReplicationEvent);

        // RU_COMPAT_5_3
        if (out.getVersion().isGreaterOrEqual(V5_4)) {
            if (noWanReplicationKeys == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeByteArray(noWanReplicationKeys.toByteArray());
            }
        }
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        int size = in.readInt();
        List keyRecordExpiry = new ArrayList<>(size * 3);
        for (int i = 0; i < size; i++) {
            Data dataKey = IOUtil.readData(in);
            Record record = Records.readRecord(in);
            ExpiryMetadata expiryMetadata = Records.readExpiry(in);

            keyRecordExpiry.add(dataKey);
            keyRecordExpiry.add(record);
            keyRecordExpiry.add(expiryMetadata);
        }
        this.keyRecordExpiry = keyRecordExpiry;
        this.disableWanReplicationEvent = in.readBoolean();
        this.noWanReplicationKeys = null;

        // RU_COMPAT_5_3
        if (in.getVersion().isGreaterOrEqual(V5_4)) {
            if (in.readBoolean()) {
                this.noWanReplicationKeys = BitSet.valueOf(in.readByteArray());
            }
        }
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_ALL_BACKUP;
    }
}
