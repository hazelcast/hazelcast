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
 * Operation implementation for {@link com.hazelcast.cache.impl.ICacheRecordStore#put(Data, Object, ExpiryPolicy, String)} and
 * {@link com.hazelcast.cache.impl.ICacheRecordStore#getAndPut(Data, Object, ExpiryPolicy, String)}.
 * @see com.hazelcast.cache.impl.ICacheRecordStore#put(Data, Object, ExpiryPolicy, String)
 * @see com.hazelcast.cache.impl.ICacheRecordStore#getAndPut(Data, Object, ExpiryPolicy, String)
 */
public class CachePutOperation
        extends AbstractMutatingCacheOperation {

    private Data value;
    // getAndPut
    private boolean get;
    private ExpiryPolicy expiryPolicy;

    public CachePutOperation() {
    }

    public CachePutOperation(String name, Data key, Data value, ExpiryPolicy expiryPolicy, boolean get) {
        this(name, key, value, expiryPolicy, get, IGNORE_COMPLETION);
    }

    public CachePutOperation(String name, Data key, Data value, ExpiryPolicy expiryPolicy, boolean get, int completionId) {
        super(name, key, completionId);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
        this.get = get;
    }

    @Override
    public void run()
            throws Exception {
        if (get) {
            response = cache.getAndPut(key, value, expiryPolicy, getCallerUuid());
        } else {
            cache.put(key, value, expiryPolicy, getCallerUuid());
            response = null;
        }
        backupRecord = cache.getRecord(key);
    }

    @Override
    public boolean shouldBackup() {
        return true;
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
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        get = in.readBoolean();
        expiryPolicy = in.readObject();
        value = in.readData();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.PUT;
    }

}
