/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;

/**
 * Base Cache Operation. Cache operations are named operations. Key based operations are subclasses of this base
 * class providing a cacheRecordStore access and partial backup support.
 */
public abstract class AbstractCacheOperation
        extends AbstractNamedOperation
        implements PartitionAwareOperation, ServiceNamespaceAware, IdentifiedDataSerializable {

    protected Data key;
    protected Object response;

    protected transient ICacheService cacheService;
    protected transient ICacheRecordStore cache;
    protected transient CacheWanEventPublisher wanEventPublisher;
    protected transient CacheRecord backupRecord;

    protected AbstractCacheOperation() {
    }

    protected AbstractCacheOperation(String name, Data key) {
        super(name);
        this.key = key;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public void beforeRun() throws Exception {
        cacheService = getService();
        cache = cacheService.getOrCreateRecordStore(name, getPartitionId());
        if (cache.isWanReplicationEnabled()) {
            wanEventPublisher = cacheService.getCacheWanEventPublisher();
        }
    }

    @Override
    public final Object getResponse() {
        return response;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof CacheNotExistsException) {
            ICacheService cacheService = getService();
            if (cacheService.getCacheConfig(name) != null) {
                getLogger().finest("Retry Cache Operation from node " + getNodeEngine().getLocalMember());
                return ExceptionAction.RETRY_INVOCATION;
            }
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof CacheNotExistsException) {
            // since this exception can be thrown and will be retried, we don't want to log this exception under server,
            // to reduce the logging noise
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("failed to execute: " + this, e);
            }
            return;
        }
        super.logError(e);
    }

    @Override
    public ObjectNamespace getServiceNamespace() {
        ICacheRecordStore recordStore = cache;
        if (recordStore == null) {
            ICacheService service = getService();
            recordStore = service.getOrCreateRecordStore(name, getPartitionId());
        }
        return recordStore.getObjectNamespace();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(key);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = in.readData();
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    // region BackupAwareOperation will use these
    public final int getSyncBackupCount() {
        return cache != null ? cache.getConfig().getBackupCount() : 0;
    }

    public final int getAsyncBackupCount() {
        return cache != null ? cache.getConfig().getAsyncBackupCount() : 0;
    }
}
