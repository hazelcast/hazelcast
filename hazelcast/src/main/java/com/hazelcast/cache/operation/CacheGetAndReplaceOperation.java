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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CacheGetAndReplaceOperation extends AbstractCacheOperation implements BackupAwareOperation {

    private Data value;
    private ExpiryPolicy expiryPolicy;

    public CacheGetAndReplaceOperation() {
    }

    public CacheGetAndReplaceOperation(String name, Data key, Data value,ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.value = value;
        this.expiryPolicy =expiryPolicy;
    }

    @Override
    public void run() throws Exception {
        CacheService service = getService();
        ICacheRecordStore cache = service.getOrCreateCache(name, getPartitionId());
        response = cache.getAndReplace(key, value, expiryPolicy, getCallerUuid());
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean shouldBackup() {
        return response != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, value, expiryPolicy);
    }


    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        value.writeData(out);
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = new Data();
        value.readData(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.GET_AND_REPLACE;
    }

}
