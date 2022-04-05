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

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.spi.tenantcontrol.TenantControl;

import static com.hazelcast.cache.impl.CacheEntryViews.createDefaultEntryView;
import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;

/**
 * Base Cache Operation. Cache operations are named operations. Key based operations are subclasses of this base
 * class providing a cacheRecordStore access and partial backup support.
 */
public abstract class CacheOperation extends AbstractNamedOperation
        implements PartitionAwareOperation, ServiceNamespaceAware, IdentifiedDataSerializable {

    protected transient boolean dontCreateCacheRecordStoreIfNotExist;
    protected transient ICacheService cacheService;
    protected transient ICacheRecordStore recordStore;
    protected transient CacheWanEventPublisher wanEventPublisher;

    protected CacheOperation() {
    }

    protected CacheOperation(String name) {
        this(name, false);
    }

    protected CacheOperation(String name, boolean dontCreateCacheRecordStoreIfNotExist) {
        super(name);
        this.dontCreateCacheRecordStoreIfNotExist = dontCreateCacheRecordStoreIfNotExist;
    }

    @Override
    public final String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public final void beforeRun() throws Exception {
        cacheService = getService();
        try {
            recordStore = getOrCreateStoreIfAllowed();
        } catch (CacheNotExistsException e) {
            dispose();
            rethrowOrSwallowIfBackup(e);
        } catch (Throwable t) {
            dispose();
            throw ExceptionUtil.rethrow(t, Exception.class);
        }

        if (recordStore != null && recordStore.isWanReplicationEnabled()) {
            wanEventPublisher = cacheService.getCacheWanEventPublisher();
            cacheService.doPrepublicationChecks(name);
        }

        beforeRunInternal();
    }

    /**
     * If a backup operation wants to get a deleted cache, swallows
     * exception by only logging it.
     *
     * If it is not a backup operation, just rethrows exception.
     */
    private void rethrowOrSwallowIfBackup(CacheNotExistsException e) throws Exception {
        if (this instanceof BackupOperation) {
            getLogger().finest("Error while getting a cache", e);
        } else {
            throw ExceptionUtil.rethrow(e, Exception.class);
        }
    }

    private ICacheRecordStore getOrCreateStoreIfAllowed() {
        if (dontCreateCacheRecordStoreIfNotExist) {
            return cacheService.getRecordStore(name, getPartitionId());
        }

        return cacheService.getOrCreateRecordStore(name, getPartitionId());
    }

    /**
     * Implementers can override to release associated resources upon a
     * successful execution or failure.
     */
    protected void dispose() {
        // NOP
    }

    /**
     * Implementers can override this method to make a specific execution.
     */
    protected void beforeRunInternal() {
        // NOP
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
    public final ObjectNamespace getServiceNamespace() {
        if (recordStore == null) {
            ICacheService service = getService();
            recordStore = service.getOrCreateRecordStore(name, getPartitionId());
        }
        return recordStore.getObjectNamespace();
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    // region BackupAwareOperation will use these
    public final int getSyncBackupCount() {
        return recordStore != null ? recordStore.getConfig().getBackupCount() : 0;
    }

    public final int getAsyncBackupCount() {
        return recordStore != null ? recordStore.getConfig().getAsyncBackupCount() : 0;
    }

    protected final void publishWanUpdate(Data dataKey, CacheRecord record) {
        if (!recordStore.isWanReplicationEnabled() || record == null) {
            return;
        }

        NodeEngine nodeEngine = getNodeEngine();
        SerializationService serializationService = nodeEngine.getSerializationService();
        Data dataValue = toHeapData(serializationService.toData(record.getValue()));
        publishWanUpdate(dataKey, dataValue, record);
    }

    protected final void publishWanUpdate(Data dataKey, Data dataValue, CacheRecord record) {
        if (!recordStore.isWanReplicationEnabled() || record == null) {
            return;
        }
        NodeEngine nodeEngine = getNodeEngine();
        SerializationService serializationService = nodeEngine.getSerializationService();

        Data dataExpiryPolicy = toHeapData(serializationService.toData(record.getExpiryPolicy()));
        publishWanUpdate(dataKey, dataValue, dataExpiryPolicy, record);
    }

    protected final void publishWanUpdate(Data dataKey, Data dataValue, Data dataExpiryPolicy, CacheRecord record) {
        assert dataValue != null;

        if (!recordStore.isWanReplicationEnabled() || record == null) {
            return;
        }

        CacheEntryView<Data, Data> entryView = createDefaultEntryView(toHeapData(dataKey),
                toHeapData(dataValue), toHeapData(dataExpiryPolicy), record);
        wanEventPublisher.publishWanUpdate(name, entryView);
    }

    protected final void publishWanRemove(Data dataKey) {
        if (!recordStore.isWanReplicationEnabled()) {
            return;
        }

        wanEventPublisher.publishWanRemove(name, toHeapData(dataKey));
    }

    boolean isObjectInMemoryFormat() {
        CacheService cacheService = getService();
        CacheConfig cacheConfig = cacheService.getCacheConfig(name);
        return (cacheConfig != null
                && cacheConfig.getInMemoryFormat() == InMemoryFormat.OBJECT);
    }

    @Override
    public TenantControl getTenantControl() {
        return getNodeEngine().getTenantControlService()
                              .getTenantControl(ICacheService.SERVICE_NAME, name);
    }
}
