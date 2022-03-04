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
 * Operator implementation for cache replace functionality.
 *
 * @see com.hazelcast.cache.impl.ICacheRecordStore#replace(Data, Object, ExpiryPolicy, java.util.UUID, int)
 * @see com.hazelcast.cache.impl.ICacheRecordStore#replace(Data, Object, Object, ExpiryPolicy, java.util.UUID, int)
 */
public class CacheReplaceOperation extends MutatingCacheOperation {

    private Data newValue;
    // replace if same
    private Data oldValue;
    private ExpiryPolicy expiryPolicy;

    public CacheReplaceOperation() {
    }

    public CacheReplaceOperation(String cacheNameWithPrefix, Data key, Data oldValue, Data newValue, ExpiryPolicy expiryPolicy,
                                 int completionId) {
        super(cacheNameWithPrefix, key, completionId);
        this.newValue = newValue;
        this.oldValue = oldValue;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public void run()
            throws Exception {
        if (oldValue == null) {
            response = recordStore.replace(key, newValue, expiryPolicy, getCallerUuid(), completionId);
        } else {
            response = recordStore.replace(key, oldValue, newValue, expiryPolicy, getCallerUuid(), completionId);
        }
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
        IOUtil.writeData(out, newValue);
        IOUtil.writeData(out, oldValue);
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        newValue = IOUtil.readData(in);
        oldValue = IOUtil.readData(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.REPLACE;
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
