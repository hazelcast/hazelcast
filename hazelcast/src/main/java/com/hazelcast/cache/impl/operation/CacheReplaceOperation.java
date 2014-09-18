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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * Cache Replace Operation
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
        newValue.writeData(out);
        IOUtil.writeNullableData(out, oldValue);
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        newValue = new Data();
        newValue.readData(in);
        oldValue = IOUtil.readNullableData(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.REPLACE;
    }

}
