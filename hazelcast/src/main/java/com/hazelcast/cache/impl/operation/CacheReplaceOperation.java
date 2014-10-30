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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Operator implementation for cache replace functionality.
 * @see com.hazelcast.cache.impl.ICacheRecordStore#replace(Data, Object, ExpiryPolicy, String)
 * @see com.hazelcast.cache.impl.ICacheRecordStore#replace(Data, Object, Object, ExpiryPolicy, String)
 */
public class CacheReplaceOperation
        extends AbstractMutatingCacheOperation {

    private Data newValue;
    // replace if same
    private Data oldValue;
    private ExpiryPolicy expiryPolicy;

    public CacheReplaceOperation() {
    }

    public CacheReplaceOperation(String name, Data key, Data oldValue, Data newValue, ExpiryPolicy expiryPolicy) {
        this(name, key, oldValue, newValue, expiryPolicy, IGNORE_COMPLETION);
    }

    public CacheReplaceOperation(String name, Data key, Data oldValue, Data newValue, ExpiryPolicy expiryPolicy,
                                 int completionId) {
        super(name, key, completionId);
        this.newValue = newValue;
        this.oldValue = oldValue;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public void run()
            throws Exception {
        if (oldValue == null) {
            response = cache.replace(key, newValue, expiryPolicy, getCallerUuid());
        } else {
            response = cache.replace(key, oldValue, newValue, expiryPolicy, getCallerUuid());
        }
        if (Boolean.TRUE.equals(response)) {
            backupRecord = cache.getRecord(key);
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, backupRecord);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeData(newValue);
        out.writeData(oldValue);
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        newValue = in.readData();
        oldValue = in.readData();
        expiryPolicy = in.readObject();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.REPLACE;
    }

}
