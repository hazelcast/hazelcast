/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Operation implementation for
 * {@link com.hazelcast.cache.impl.ICacheRecordStore#put(Data, Object, ExpiryPolicy, java.util.UUID, int)} and
 * {@link com.hazelcast.cache.impl.ICacheRecordStore#getAndPut(Data, Object, ExpiryPolicy, java.util.UUID, int)}.
 *
 * @see com.hazelcast.cache.impl.ICacheRecordStore#put(Data, Object, ExpiryPolicy, java.util.UUID, int)
 * @see com.hazelcast.cache.impl.ICacheRecordStore#getAndPut(Data, Object, ExpiryPolicy, java.util.UUID, int)
 */
public class CachePutOperation extends MutatingCacheOperation {

    private Data value;
    // getAndPut
    private boolean get;
    private ExpiryPolicy expiryPolicy;

    public CachePutOperation() {
    }

    public CachePutOperation(String cacheNameWithPrefix, Data key, Data value,
                             ExpiryPolicy expiryPolicy, boolean get, int completionId) {
        super(cacheNameWithPrefix, key, completionId);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
        this.get = get;
    }

    @Override
    public void run()
            throws Exception {
        if (get) {
            response = recordStore.getAndPut(key, value, expiryPolicy, getCallerUuid(), completionId);
            backupRecord = recordStore.getRecord(key);
        } else {
            backupRecord = recordStore.put(key, value, expiryPolicy, getCallerUuid(), completionId);
        }
    }

    @Override
    public void afterRun() throws Exception {
        publishWanUpdate(key, backupRecord);
        super.afterRun();
    }

    @Override
    public boolean shouldBackup() {
        // Backup record may be null since record store might be cleared by destroy operation at the same time
        // because destroy operation is not called from partition thread pool.
        // In this case, we simply ignore backup operation
        // because record store on backup will be cleared also by destroy operation.
        return backupRecord != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, backupRecord);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeBoolean(get);
        out.writeObject(expiryPolicy);
        IOUtil.writeData(out, value);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        get = in.readBoolean();
        expiryPolicy = in.readObject();
        value = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.PUT;
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
