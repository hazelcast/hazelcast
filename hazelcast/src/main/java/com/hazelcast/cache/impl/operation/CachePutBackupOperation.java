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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * Cache PutBackup Operation, backup operation for operation adding cache entries into record stores.
 * @see CacheEntryProcessorOperation
 * @see CachePutOperation
 * @see CachePutIfAbsentOperation
 * @see CacheReplaceOperation
 * @see CacheGetAndReplaceOperation
 */
public class CachePutBackupOperation
        extends AbstractCacheOperation
        implements BackupOperation {

    private CacheRecord cacheRecord;

    public CachePutBackupOperation() {
    }

    public CachePutBackupOperation(String name, Data key, CacheRecord cacheRecord) {
        super(name, key);
        this.cacheRecord = cacheRecord;
    }

    @Override
    public void run()
            throws Exception {
        CacheService service = getService();
        ICacheRecordStore cache = service.getOrCreateCache(name, getPartitionId());
        cache.setRecord(key, cacheRecord);
        response = Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(cacheRecord);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        cacheRecord = in.readObject();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.PUT_BACKUP;
    }

}
