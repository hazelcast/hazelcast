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
 * Operation implementation for calling
 * {@link com.hazelcast.cache.impl.ICacheRecordStore#putIfAbsent(Data, Object, ExpiryPolicy, java.util.UUID, int)}.
 *
 * @see com.hazelcast.cache.impl.ICacheRecordStore#putIfAbsent(Data, Object, ExpiryPolicy, java.util.UUID, int)
 */
public class CachePutIfAbsentOperation extends MutatingCacheOperation {

    private Data value;
    private ExpiryPolicy expiryPolicy;

    public CachePutIfAbsentOperation() {
    }

    public CachePutIfAbsentOperation(String cacheNameWithPrefix, Data key, Data value,
                                     ExpiryPolicy expiryPolicy, int completionId) {
        super(cacheNameWithPrefix, key, completionId);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public void run()
            throws Exception {
        response = recordStore.putIfAbsent(key, value, expiryPolicy, getCallerUuid(), completionId);
        if (Boolean.TRUE.equals(response)) {
            backupRecord = recordStore.getRecord(key);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            publishWanUpdate(key, backupRecord);
        }
        super.afterRun();
    }

    @Override
    public boolean shouldBackup() {
        // Backup record may be null since record store might be cleared by destroy operation at the same time
        // because destroy operation is not called from partition thread pool.
        // In this case, we simply ignore backup operation
        // because record store on backup will be cleared also by destroy operation.
        return Boolean.TRUE.equals(response) && backupRecord != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, backupRecord);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
        IOUtil.writeData(out, value);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();
        value = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.PUT_IF_ABSENT;
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
