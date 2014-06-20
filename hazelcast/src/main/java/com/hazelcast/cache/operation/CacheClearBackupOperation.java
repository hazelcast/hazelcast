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

package com.hazelcast.cache.operation;

import com.hazelcast.cache.CacheDataSerializerHook;
import com.hazelcast.cache.CacheService;
import com.hazelcast.cache.ICacheRecordStore;
import com.hazelcast.cache.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CacheClearBackupOperation extends AbstractNamedOperation implements BackupOperation, IdentifiedDataSerializable {

    private Map<Data, CacheRecord> backupRecord;

    private transient ICacheRecordStore cache;

    public CacheClearBackupOperation() {
    }

    public CacheClearBackupOperation(String name, Map<Data, CacheRecord> backupRecord) {
        super(name);
        this.backupRecord = backupRecord;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CLEAR_BACKUP;
    }

    @Override
    public void beforeRun() throws Exception {
        CacheService service = getService();
        cache = service.getOrCreateCache(name, getPartitionId());
//        cache.setRecordStoreMode(false);
    }

    @Override
    public void run() throws Exception {
        if (backupRecord != null) {
            for (Map.Entry<Data, CacheRecord> entry : backupRecord.entrySet()) {
                cache.own(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
//        cache.setRecordStoreMode(true);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(backupRecord != null);
        if (backupRecord != null) {
            out.write(backupRecord.size());
            for (Map.Entry<Data, CacheRecord> entry : backupRecord.entrySet()) {
                final Data key = entry.getKey();
                final CacheRecord record = entry.getValue();
                key.writeData(out);
                out.writeObject(record);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final boolean recordNotNull = in.readBoolean();
        if (recordNotNull) {
            int size = in.readInt();
            backupRecord = new HashMap<Data, CacheRecord>(size);
            for (int i = 0; i < size; i++) {
                final Data key = new Data();
                key.readData(in);
                final CacheRecord record = in.readObject();
                backupRecord.put(key, record);
            }
        }
    }


}
