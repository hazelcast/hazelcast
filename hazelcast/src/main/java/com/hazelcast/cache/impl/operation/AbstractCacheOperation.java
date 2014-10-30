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

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;

/**
 * Base Cache Operation. Cache operations are named operations. Key based operations are subclasses of this base
 * class providing a cacheRecordStore access and partial backup support.
 */
abstract class AbstractCacheOperation
        extends AbstractNamedOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    Data key;
    Object response;

    transient ICacheRecordStore cache;

    transient CacheRecord backupRecord;

    protected AbstractCacheOperation() {
    }

    protected AbstractCacheOperation(String name, Data key) {
        super(name);
        this.key = key;
    }

    @Override
    public final void beforeRun()
            throws Exception {
        CacheService service = getService();
        cache = service.getOrCreateCache(name, getPartitionId());
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
    public ExceptionAction onException(Throwable throwable) {
        if (throwable instanceof CacheNotExistsException) {
            CacheService cacheService = getService();
            if (cacheService.getCacheConfig(name) != null) {
                getLogger().finest("Retry Cache Operation from node " + getNodeEngine().getLocalMember());
                return ExceptionAction.RETRY_INVOCATION;
            }
        }
        return super.onException(throwable);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeData(key);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        key = in.readData();
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
