/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapDataStores;
import com.hazelcast.map.impl.mapstore.writebehind.TxnReservedCapacityCounter;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.operation.steps.IMapStepAwareOperation;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.StepRunner;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.map.impl.wan.WanMapEntryView;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.tenantcontrol.TenantControl;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.logging.Level;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.EntryViews.createWanEntryView;
import static com.hazelcast.map.impl.operation.ForcedEviction.runWithForcedEvictionStrategies;
import static com.hazelcast.map.impl.operation.steps.engine.StepRunner.isStepRunnerCurrentlyExecutingOnPartitionThread;
import static com.hazelcast.spi.impl.operationservice.CallStatus.RESPONSE;
import static com.hazelcast.spi.impl.operationservice.CallStatus.VOID;
import static com.hazelcast.spi.impl.operationservice.CallStatus.WAIT;

@SuppressWarnings("checkstyle:methodcount")
public abstract class MapOperation extends AbstractNamedOperation
        implements IdentifiedDataSerializable, ServiceNamespaceAware,
        IMapStepAwareOperation {

    private static final boolean ASSERTION_ENABLED = MapOperation.class.desiredAssertionStatus();

    protected transient MapService mapService;
    protected transient RecordStore<Record> recordStore;
    protected transient MapContainer mapContainer;
    protected transient MapServiceContext mapServiceContext;
    protected transient MapEventPublisher mapEventPublisher;

    protected transient boolean createRecordStoreOnDemand = true;
    protected transient boolean disposeDeferredBlocks = true;
    protected transient boolean tieredStoreAndPartitionCompactorEnabled;
    protected transient boolean mapStoreOffloadEnabled;

    private transient boolean canPublishWanEvent;

    public MapOperation() {
    }

    public MapOperation(String name) {
        this.name = name;
    }

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public final void beforeRun() throws Exception {
        super.beforeRun();

        mapService = getService();
        mapServiceContext = mapService.getMapServiceContext();
        mapEventPublisher = mapServiceContext.getMapEventPublisher();

        try {
            recordStore = getRecordStoreOrNull();
            if (recordStore == null) {
                mapContainer = mapServiceContext.getMapContainer(name);
            } else {
                mapContainer = recordStore.getMapContainer();
            }
        } catch (Throwable t) {
            disposeDeferredBlocks();
            throw rethrow(t, Exception.class);
        }

        canPublishWanEvent = canPublishWanEvent(mapContainer);

        MapConfig mapConfig = mapContainer.getMapConfig();
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();

        boolean hasUserConfiguredOffload = mapServiceContext.isForceOffloadEnabled()
                || (mapStoreConfig.isOffload()
                && recordStore != null
                && recordStore.getMapDataStore() != MapDataStores.EMPTY_MAP_DATA_STORE);

        // check if mapStoreOffloadEnabled is true for this operation
        mapStoreOffloadEnabled = recordStore != null
                && hasUserConfiguredOffload
                && getStartingStep() != null
                && !mapConfig.getTieredStoreConfig().isEnabled();

        // check if tieredStore and partitionCompactor
        // are both enabled for this operation
        tieredStoreAndPartitionCompactorEnabled = recordStore != null
                && recordStore.getStorage().isPartitionCompactorEnabled()
                && getStartingStep() != null
                && mapConfig.getTieredStoreConfig().isEnabled();

        assertOnlyOneOfMapStoreOrTieredStoreEnabled();
        assertNativeMapOnPartitionThread();

        innerBeforeRun();
    }

    // Currently we don't allow both map-store and
    // tiered-store configured for the same map
    private void assertOnlyOneOfMapStoreOrTieredStoreEnabled() {
        if (!ASSERTION_ENABLED) {
            return;
        }

        if (mapStoreOffloadEnabled) {
            assert !tieredStoreAndPartitionCompactorEnabled;
            return;
        }

        if (tieredStoreAndPartitionCompactorEnabled) {
            assert !mapStoreOffloadEnabled;
            return;
        }
    }

    @Nullable
    public RecordStore<Record> getRecordStore() {
        return recordStore;
    }

    protected void innerBeforeRun() throws Exception {
        // when tieredStoreAndPartitionCompactorEnabled is true,
        // StepSupplier calls beforeOperation and afterOperation
        if (recordStore != null
                && !tieredStoreAndPartitionCompactorEnabled) {
            recordStore.beforeOperation();
        }
        // Concrete classes can override this method.
    }

    @Override
    public final void run() {
        try {
            runInternal();
        } catch (NativeOutOfMemoryError e) {
            rerunWithForcedEviction();
        }
    }

    @Override
    public CallStatus call() throws Exception {
        if (this instanceof BlockingOperation) {
            BlockingOperation blockingOperation = (BlockingOperation) this;
            if (blockingOperation.shouldWait()) {
                return WAIT;
            }
        }

        if (isMapStoreOffloadEnabled() || tieredStoreAndPartitionCompactorEnabled) {
            assert recordStore != null;
            return offloadOperation();
        }

        run();
        return returnsResponse() ? RESPONSE : VOID;
    }

    protected final boolean isMapStoreOffloadEnabled() {
        // This is for nested calls from partition thread. When we see
        // nested call we directly run the call without offloading.
        if (mapStoreOffloadEnabled
                && ThreadUtil.isRunningOnPartitionThread()
                && isStepRunnerCurrentlyExecutingOnPartitionThread()) {
            return false;
        }
        return mapStoreOffloadEnabled;
    }

    public final boolean isTieredStoreAndPartitionCompactorEnabled() {
        // This is for nested calls from partition thread. When we see
        // nested call we directly run the call without offloading.
        if (tieredStoreAndPartitionCompactorEnabled
                && ThreadUtil.isRunningOnPartitionThread()
                && isStepRunnerCurrentlyExecutingOnPartitionThread()) {
            return false;
        }
        return tieredStoreAndPartitionCompactorEnabled;
    }

    protected Offload offloadOperation() {
        return new StepRunner(this);
    }

    @Override
    public State createState() {
        return new State(recordStore, this)
                .setPartitionId(getPartitionId())
                .setCallerAddress(getCallerAddress())
                .setCallerProvenance(getCallerProvenance())
                .setDisableWanReplicationEvent(disableWanReplicationEvent());
    }

    protected void runInternal() {
        // Intentionally empty method body.
        // Concrete classes can override this method.
    }

    public void runInternalDirect() {
        runInternal();
    }

    private void rerunWithForcedEviction() {
        try {
            runWithForcedEvictionStrategies(this);
        } catch (NativeOutOfMemoryError e) {
            disposeDeferredBlocks();
            throw e;
        }
    }

    @Override
    public final void afterRun() throws Exception {
        if (mapStoreOffloadEnabled
                || tieredStoreAndPartitionCompactorEnabled) {
            return;
        }
        afterRunInternal();
        disposeDeferredBlocks();
        super.afterRun();
    }

    public void afterRunInternal() {
        // Intentionally empty method body.
        // Concrete classes can override this method.
    }

    @Override
    public void afterRunFinal() {
        // when tieredStoreAndPartitionCompactorEnabled is
        // true, we handle afterOperation in StepSupplier
        if (!tieredStoreAndPartitionCompactorEnabled
                && recordStore != null) {
            recordStore.afterOperation();
        }
    }

    protected void assertNativeMapOnPartitionThread() {
        if (!ASSERTION_ENABLED) {
            return;
        }

        assert mapContainer.getMapConfig().getInMemoryFormat() != NATIVE
                || getPartitionId() != GENERIC_PARTITION_ID
                : "Native memory backed map operations are not allowed to run on GENERIC_PARTITION_ID";
    }

    ILogger logger() {
        return getLogger();
    }

    protected final CallerProvenance getCallerProvenance() {
        return disableWanReplicationEvent() ? CallerProvenance.WAN : CallerProvenance.NOT_WAN;
    }

    private RecordStore getRecordStoreOrNull() {
        int partitionId = getPartitionId();
        if (partitionId == -1) {
            return null;
        }
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        if (createRecordStoreOnDemand) {
            return partitionContainer.getRecordStore(name);
        } else {
            return partitionContainer.getExistingRecordStore(name);
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        disposeDeferredBlocks();
        super.onExecutionFailure(e);
    }

    @Override
    public void logError(Throwable e) {
        ILogger logger = getLogger();
        if (e instanceof NativeOutOfMemoryError) {
            Level level = this instanceof BackupOperation ? Level.FINEST : Level.WARNING;
            logger.log(level, "Cannot complete operation! -> " + e.getMessage());
        } else {
            // we need to introduce a proper method to handle operation failures (at the moment
            // this is the only place where we can dispose native memory allocations on failure)
            disposeDeferredBlocks();
            super.logError(e);
        }
    }

    public void disposeDeferredBlocks() {
        if (!disposeDeferredBlocks
                || recordStore == null
                || recordStore.getInMemoryFormat() != NATIVE) {
            return;
        }

        recordStore.disposeDeferredBlocks();
    }

    private boolean canPublishWanEvent(MapContainer mapContainer) {
        boolean canPublishWanEvent = mapContainer.isWanReplicationEnabled()
                && !disableWanReplicationEvent();

        if (canPublishWanEvent) {
            mapContainer.getWanReplicationDelegate().doPrepublicationChecks();
        }
        return canPublishWanEvent;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public boolean isPostProcessingOrHasInterceptor(RecordStore recordStore) {
        MapDataStore mapDataStore = recordStore.getMapDataStore();
        return mapDataStore.isPostProcessingMapStore()
                || !mapContainer.getInterceptorRegistry().getInterceptors().isEmpty();
    }

    public void setThreadId(long threadId) {
        throw new UnsupportedOperationException();
    }

    public long getThreadId() {
        throw new UnsupportedOperationException();
    }

    public final void invalidateNearCache(List<Data> keys) {
        if (!mapContainer.hasInvalidationListener() || isEmpty(keys)) {
            return;
        }

        Invalidator invalidator = getNearCacheInvalidator();

        for (Data key : keys) {
            invalidator.invalidateKey(key, name, getCallerUuid());
        }
    }

    // TODO: improve here it's possible that client cannot manage to attach listener
    public final void invalidateNearCache(Data key) {
        if (!mapContainer.hasInvalidationListener() || key == null) {
            return;
        }

        Invalidator invalidator = getNearCacheInvalidator();
        invalidator.invalidateKey(key, name, getCallerUuid());
    }

    /**
     * This method helps to add clearing Near Cache event only from
     * one-partition which matches partitionId of the map name.
     */
    protected final void invalidateAllKeysInNearCaches() {
        if (mapContainer.hasInvalidationListener()) {

            int partitionId = getPartitionId();
            Invalidator invalidator = getNearCacheInvalidator();

            if (partitionId == getNodeEngine().getPartitionService().getPartitionId(name)) {
                invalidator.invalidateAllKeys(name, getCallerUuid());
            } else {
                invalidator.forceIncrementSequence(name, getPartitionId());
            }
        }
    }

    private Invalidator getNearCacheInvalidator() {
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        return mapNearCacheManager.getInvalidator();
    }

    public final void evict(Data justAddedKey) {
        if (mapContainer.getEvictor() == Evictor.NULL_EVICTOR) {
            return;
        }
        recordStore.evictEntries(justAddedKey);
        disposeDeferredBlocks();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public ObjectNamespace getServiceNamespace() {
        MapContainer container = mapContainer;
        if (container == null) {
            MapService service = getService();
            container = service.getMapServiceContext().getMapContainer(name);
        }
        return container.getObjectNamespace();
    }

    // for testing only
    public void setMapService(MapService mapService) {
        this.mapService = mapService;
    }

    // for testing only
    public void setMapContainer(MapContainer mapContainer) {
        this.mapContainer = mapContainer;
    }

    public final void publishWanUpdate(Data dataKey, Object value) {
        publishWanUpdateInternal(dataKey, value, false);
    }

    private void publishWanUpdateInternal(Data dataKey, Object value, boolean hasLoadProvenance) {
        if (!canPublishWanEvent) {
            return;
        }

        Record<Object> record = recordStore.getRecord(dataKey);
        if (record == null) {
            return;
        }

        Data dataValue = toHeapData(mapServiceContext.toData(value));
        ExpiryMetadata expiryMetadata = recordStore.getExpirySystem().getExpiryMetadata(dataKey);
        WanMapEntryView<Object, Object> entryView = createWanEntryView(
                toHeapData(dataKey), dataValue, record, expiryMetadata,
                getNodeEngine().getSerializationService());

        mapEventPublisher.publishWanUpdate(name, entryView, hasLoadProvenance);
    }

    protected final void publishLoadAsWanUpdate(Data dataKey, Object value) {
        publishWanUpdateInternal(dataKey, value, true);
    }

    protected final void publishWanRemove(@Nonnull Data dataKey) {
        if (!canPublishWanEvent) {
            return;
        }

        mapEventPublisher.publishWanRemove(name, toHeapData(dataKey));
    }

    protected boolean disableWanReplicationEvent() {
        return false;
    }

    public final TxnReservedCapacityCounter wbqCapacityCounter() {
        return recordStore.getMapDataStore().getTxnReservedCapacityCounter();
    }

    public final Data getValueOrPostProcessedValue(Record record, Data dataValue) {
        if (!isPostProcessingOrHasInterceptor(recordStore)) {
            return dataValue;
        }
        return mapServiceContext.toData(record.getValue());
    }

    @Override
    public TenantControl getTenantControl() {
        return getNodeEngine().getTenantControlService()
                .getTenantControl(MapService.SERVICE_NAME, name);
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }

    public MapContainer getMapContainer() {
        return mapContainer;
    }
}
