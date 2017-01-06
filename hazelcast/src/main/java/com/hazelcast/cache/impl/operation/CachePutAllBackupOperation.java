/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.MapUtil;

import java.io.IOException;
import java.util.Map;

/**
 * Cache PutAllBackup Operation is the backup operation used by load all operation. Provides backup of
 * multiple entries.
 * @see com.hazelcast.cache.impl.operation.CacheLoadAllOperation
 */
public class CachePutAllBackupOperation
        extends AbstractNamedOperation
        implements BackupOperation, IdentifiedDataSerializable, MutatingOperation {

    private Map<Data, CacheRecord> cacheRecords;
    private transient ICacheRecordStore cache;

    public CachePutAllBackupOperation() {
    }

    public CachePutAllBackupOperation(String name, Map<Data, CacheRecord> cacheRecords) {
        super(name);
        this.cacheRecords = cacheRecords;
    }

    @Override
    public void beforeRun()
            throws Exception {
        ICacheService service = getService();
        try {
            cache = service.getOrCreateRecordStore(name, getPartitionId());
        } catch (CacheNotExistsException e) {
            getLogger().finest("Error while getting a cache", e);
        }
    }

    @Override
    public void run() throws Exception {
        if (cache == null) {
            return;
        }
        if (cacheRecords != null) {
            for (Map.Entry<Data, CacheRecord> entry : cacheRecords.entrySet()) {
                CacheRecord record = entry.getValue();
                cache.putRecord(entry.getKey(), record);
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeBoolean(cacheRecords != null);
        if (cacheRecords != null) {
            out.writeInt(cacheRecords.size());
            for (Map.Entry<Data, CacheRecord> entry : cacheRecords.entrySet()) {
                final Data key = entry.getKey();
                final CacheRecord record = entry.getValue();
                out.writeData(key);
                out.writeObject(record);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        final boolean recordNotNull = in.readBoolean();
        if (recordNotNull) {
            int size = in.readInt();
            cacheRecords = MapUtil.createHashMap(size);
            for (int i = 0; i < size; i++) {
                final Data key = in.readData();
                final CacheRecord record = in.readObject();
                cacheRecords.put(key, record);
            }
        }
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.PUT_ALL_BACKUP;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

}
