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
import com.hazelcast.cache.impl.record.WanWrappedCacheRecord;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.EOFException;
import java.io.IOException;
import java.util.BitSet;
import java.util.Map;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Cache PutAllBackup Operation is the backup operation used by load all operation. Provides backup of
 * multiple entries.
 *
 * @see com.hazelcast.cache.impl.operation.CacheLoadAllOperation
 */
public class CachePutAllBackupOperation extends CacheOperation implements BackupOperation, Versioned {

    private Map<Data, WanWrappedCacheRecord> cacheRecords;

    public CachePutAllBackupOperation() {
    }

    public CachePutAllBackupOperation(String cacheNameWithPrefix, Map<Data, WanWrappedCacheRecord> cacheRecords) {
        super(cacheNameWithPrefix);
        this.cacheRecords = cacheRecords;
    }

    @Override
    public void run() throws Exception {
        if (recordStore == null) {
            return;
        }
        if (cacheRecords != null) {
            for (Map.Entry<Data, WanWrappedCacheRecord> entry : cacheRecords.entrySet()) {
                WanWrappedCacheRecord wrapped = entry.getValue();
                recordStore.putRecord(entry.getKey(), wrapped.getRecord(), true);

                if (wrapped.isWanReplicated()) {
                    publishWanUpdate(entry.getKey(), wrapped.getRecord());
                }
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(cacheRecords != null);
        if (cacheRecords != null) {
            out.writeInt(cacheRecords.size());
            BitSet nonWanReplicatedKeys = new BitSet(cacheRecords.size());
            int index = 0;
            for (Map.Entry<Data, WanWrappedCacheRecord> entry : cacheRecords.entrySet()) {
                Data key = entry.getKey();
                WanWrappedCacheRecord wrapped = entry.getValue();
                IOUtil.writeData(out, key);
                out.writeObject(wrapped.getRecord());

                if (!wrapped.isWanReplicated()) {
                    nonWanReplicatedKeys.set(index);
                }
                index++;
            }

            if (nonWanReplicatedKeys.isEmpty()) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeByteArray(nonWanReplicatedKeys.toByteArray());
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        boolean recordNotNull = in.readBoolean();
        if (recordNotNull) {
            int size = in.readInt();
            cacheRecords = createHashMap(size);

            Data[] orderedKeys = new Data[size];
            for (int i = 0; i < size; i++) {
                Data key = IOUtil.readData(in);
                CacheRecord record = in.readObject();
                orderedKeys[i] = key;
                cacheRecords.put(key, new WanWrappedCacheRecord(record, true));
            }

            // RU_COMPAT_5_3
            try {
                if (in.readBoolean()) {
                    BitSet nonWanKeys = BitSet.valueOf(in.readByteArray());
                    for (int i = nonWanKeys.nextSetBit(0); i >= 0; i = nonWanKeys.nextSetBit(i + 1)) {
                        cacheRecords.get(orderedKeys[i]).setWanReplicated(false);
                    }
                }
            } catch (EOFException ex) {
                ignore(ex);
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
