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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
abstract class AbstractCacheOperation extends AbstractNamedOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    Data key;
    Object response;

    transient ICacheRecordStore cache;

    protected AbstractCacheOperation() {
    }

    protected AbstractCacheOperation(String name, Data key) {
        super(name);
        this.key = key;
    }

    @Override
    public final void beforeRun() throws Exception {
        CacheService service = getService();
        if (this instanceof BackupAwareOperation) {
            cache = service.getOrCreateCache(name, getPartitionId());
        } else {
            cache = service.getCache(name, getPartitionId());
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public final Object getResponse() {
        return response;
    }


    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        key.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = new Data();
        key.readData(in);
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    //region BackupawareOperation will use these
    public final int getSyncBackupCount() {
        return cache != null ? cache.getConfig().getBackupCount() : 0;
    }

    public final int getAsyncBackupCount() {
        return cache != null ? cache.getConfig().getAsyncBackupCount() : 0;
    }
}
