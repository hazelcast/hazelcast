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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static com.hazelcast.internal.cluster.Versions.V5_4;

/**
 * Cache PutAllBackup Operation is the backup operation used by load all operation. Provides backup of
 * multiple entries.
 *
 * @see com.hazelcast.cache.impl.operation.CacheLoadAllOperation
 */
public class CachePutAllBackupOperation extends CacheOperation implements BackupOperation, Versioned {

    private List dataCacheRecordPairs;
    @Nullable
    private BitSet noWanReplicationKeys;

    private transient int lastIndex;

    public CachePutAllBackupOperation() {
    }

    public CachePutAllBackupOperation(String cacheNameWithPrefix, List dataCacheRecordPairs, BitSet noWanReplicationKeys) {
        super(cacheNameWithPrefix);
        this.dataCacheRecordPairs = dataCacheRecordPairs;
        this.noWanReplicationKeys = noWanReplicationKeys;
    }

    public CachePutAllBackupOperation(String cacheNameWithPrefix, List dataCacheRecordPairs) {
        this(cacheNameWithPrefix, dataCacheRecordPairs, null);
    }

    @Override
    public void run() throws Exception {
        if (recordStore == null) {
            return;
        }
        if (dataCacheRecordPairs != null) {
            for (int i = lastIndex; i < dataCacheRecordPairs.size(); i += 2) {
                Data key = (Data) dataCacheRecordPairs.get(i);
                CacheRecord record = (CacheRecord) dataCacheRecordPairs.get(i + 1);
                recordStore.putRecord(key, record, true);

                boolean wanReplicated = noWanReplicationKeys == null || !noWanReplicationKeys.get(i / 2);
                if (wanReplicated) {
                    publishWanUpdate(key, record);
                }
                lastIndex = i;
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(dataCacheRecordPairs != null);
        if (dataCacheRecordPairs != null) {
            out.writeInt(dataCacheRecordPairs.size() / 2);
            for (int i = 0; i < dataCacheRecordPairs.size(); i += 2) {
                Data dataKey = (Data) dataCacheRecordPairs.get(i);
                CacheRecord record = (CacheRecord) dataCacheRecordPairs.get(i + 1);

                IOUtil.writeData(out, dataKey);
                out.writeObject(record);
            }

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
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        boolean recordNotNull = in.readBoolean();
        if (recordNotNull) {
            int size = in.readInt();
            dataCacheRecordPairs = new ArrayList(size * 2);

            for (int i = 0; i < size; i++) {
                Data key = IOUtil.readData(in);
                CacheRecord record = in.readObject();
                dataCacheRecordPairs.add(key);
                dataCacheRecordPairs.add(record);
            }

            // RU_COMPAT_5_3
            if (in.getVersion().isGreaterOrEqual(V5_4)) {
                if (in.readBoolean()) {
                    this.noWanReplicationKeys = BitSet.valueOf(in.readByteArray());
                }
            }
        }
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.PUT_ALL_BACKUP;
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
