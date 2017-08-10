/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.LockAwareLazyMapEntry;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.FalsePredicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.internal.nearcache.impl.invalidation.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;

/**
 * Operator for single key processing logic of {@link EntryProcessor}/{@link EntryBackupProcessor} related operations.
 */
public final class EntryOperator {

    private final boolean collectWanEvents;
    private final boolean shouldClone;
    private final boolean backup;
    private final boolean readOnly;
    private final boolean wanReplicationEnabled;
    private final boolean hasEventRegistration;
    private final int partitionId;
    private final long startTimeNanos = System.nanoTime();
    private final String mapName;
    private final RecordStore recordStore;
    private final InternalSerializationService ss;
    private final MapContainer mapContainer;
    private final MapEventPublisher mapEventPublisher;
    private final LocalMapStatsImpl stats;
    private final IPartitionService partitionService;
    private final Predicate predicate;
    private final MapServiceContext mapServiceContext;
    private final MapOperation mapOperation;
    private final Address callerAddress;
    private final List<WanEventHolder> wanEventList;
    private final InMemoryFormat inMemoryFormat;

    private EntryProcessor entryProcessor;
    private EntryBackupProcessor backupProcessor;

    // these fields can be used to reset this operator
    private Data dataKey;
    private Object oldValue;
    private Object newValue;
    private EntryEventType eventType;
    private Data result;

    @SuppressWarnings("checkstyle:executablestatementcount")
    private EntryOperator(MapOperation mapOperation, Object processor, Predicate predicate, boolean collectWanEvents) {
        this.backup = mapOperation instanceof BackupOperation;
        setProcessor(processor);
        this.mapOperation = mapOperation;
        this.predicate = predicate;
        this.recordStore = mapOperation.recordStore;
        this.collectWanEvents = collectWanEvents;
        this.readOnly = entryProcessor instanceof ReadOnly;
        this.wanEventList = collectWanEvents ? new ArrayList<WanEventHolder>() : Collections.<WanEventHolder>emptyList();
        this.mapContainer = recordStore.getMapContainer();
        this.inMemoryFormat = mapContainer.getMapConfig().getInMemoryFormat();
        this.mapName = mapContainer.getName();
        this.wanReplicationEnabled = mapContainer.isWanReplicationEnabled();
        this.shouldClone = mapContainer.shouldCloneOnEntryProcessing(mapOperation.getPartitionId());
        this.mapServiceContext = mapContainer.getMapServiceContext();
        LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
        this.stats = localMapStatsProvider.getLocalMapStatsImpl(mapName);
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.ss = ((InternalSerializationService) nodeEngine.getSerializationService());
        this.partitionService = nodeEngine.getPartitionService();
        EventService eventService = nodeEngine.getEventService();
        this.hasEventRegistration = eventService.hasEventRegistration(SERVICE_NAME, mapName);
        this.mapEventPublisher = mapServiceContext.getMapEventPublisher();
        this.partitionId = recordStore.getPartitionId();
        this.callerAddress = mapOperation.getCallerAddress();
    }

    private void setProcessor(Object processor) {
        if (backup) {
            backupProcessor = ((EntryBackupProcessor) processor);
            entryProcessor = null;
        } else {
            entryProcessor = ((EntryProcessor) processor);
            backupProcessor = null;
        }
    }

    public static EntryOperator operator(MapOperation mapOperation) {
        return new EntryOperator(mapOperation, null, null, false);
    }

    public static EntryOperator operator(MapOperation mapOperation, Object processor) {
        return new EntryOperator(mapOperation, processor, null, false);
    }

    public static EntryOperator operator(MapOperation mapOperation, Object processor, Predicate predicate) {
        return new EntryOperator(mapOperation, processor, predicate, false);
    }

    public static EntryOperator operator(MapOperation mapOperation, Object processor, Predicate predicate,
                                         boolean collectWanEvents) {
        return new EntryOperator(mapOperation, processor, predicate, collectWanEvents);
    }

    public EntryOperator init(Data dataKey, Object oldValue, Object newValue, Data result, EntryEventType eventType) {
        this.dataKey = dataKey;
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.eventType = eventType;
        this.result = result;
        return this;
    }

    public EntryOperator operateOnKey(Data dataKey) {
        init(dataKey, null, null, null, null);

        if (belongsAnotherPartition(dataKey)) {
            return this;
        }

        oldValue = recordStore.get(dataKey, backup);
        Boolean locked = recordStore.isLocked(dataKey);

        return operateOnKeyValueInternal(dataKey, clonedOrRawOldValue(), locked);
    }

    public EntryOperator operateOnKeyValue(Data dataKey, Object oldValue) {
        return operateOnKeyValueInternal(dataKey, oldValue, null);
    }

    private EntryOperator operateOnKeyValueInternal(Data dataKey, Object oldValue, Boolean locked) {
        init(dataKey, oldValue, null, null, null);

        Map.Entry entry = createMapEntry(dataKey, oldValue, locked);
        if (outOfPredicateScope(entry)) {
            return this;
        }

        process(entry);
        findModificationType(entry);
        newValue = entry.getValue();

        if (readOnly && entryWasModified()) {
            throwModificationInReadOnlyException();
        }
        return this;
    }

    private boolean entryWasModified() {
        return eventType != null;
    }

    public EntryEventType getEventType() {
        return eventType;
    }

    public List<WanEventHolder> getWanEventList() {
        return wanEventList;
    }

    public Object getNewValue() {
        return newValue;
    }

    public Object getOldValue() {
        return oldValue;
    }

    public Data getResult() {
        return result;
    }

    public EntryOperator doPostOperateOps() {
        if (eventType == null) {
            // when event type is null, it means this is a read-only entry processor and not modified entry.
            return this;
        }
        switch (eventType) {
            case ADDED:
            case UPDATED:
                onAddedOrUpdated();
                break;
            case REMOVED:
                onRemove();
                break;
            default:
                throw new IllegalArgumentException("Unexpected event found:" + eventType);
        }

        if (wanReplicationEnabled) {
            publishWanReplicationEvent();
        }

        if (!backup) {
            if (hasEventRegistration) {
                publishEntryEvent();
            }
            mapOperation.invalidateNearCache(dataKey);
        }

        mapOperation.evict(dataKey);
        return this;
    }

    private Object clonedOrRawOldValue() {
        return shouldClone ? ss.toObject(ss.toData(oldValue)) : oldValue;
    }

    // Needed for MultipleEntryOperation.
    private boolean belongsAnotherPartition(Data key) {
        return partitionService.getPartitionId(key) != partitionId;
    }

    private boolean outOfPredicateScope(Map.Entry entry) {
        assert entry instanceof QueryableEntry;

        if (predicate == null || predicate == TruePredicate.INSTANCE) {
            return false;
        }

        return predicate == FalsePredicate.INSTANCE || !predicate.apply(entry);
    }

    private Map.Entry createMapEntry(Data key, Object value, Boolean locked) {
        return new LockAwareLazyMapEntry(key, value, ss, mapContainer.getExtractors(), locked);
    }

    private void findModificationType(Map.Entry entry) {
        LazyMapEntry lazyMapEntry = (LazyMapEntry) entry;

        if (!lazyMapEntry.isModified()
                || (oldValue == null && lazyMapEntry.hasNullValue())) {
            // read only
            eventType = null;
            return;
        }

        if (lazyMapEntry.hasNullValue()) {
            eventType = REMOVED;
            return;
        }

        eventType = oldValue == null ? ADDED : UPDATED;
    }

    private void onAddedOrUpdated() {
        if (backup) {
            recordStore.putBackup(dataKey, newValue);
        } else {
            recordStore.set(dataKey, newValue, DEFAULT_TTL);
            if (mapOperation.isPostProcessing(recordStore)) {
                Record record = recordStore.getRecord(dataKey);
                newValue = record == null ? null : record.getValue();
            }
            mapServiceContext.interceptAfterPut(mapName, newValue);
            stats.incrementPutLatencyNanos(getLatencyNanos(startTimeNanos));
        }
    }

    private void onRemove() {
        if (backup) {
            recordStore.removeBackup(dataKey);
        } else {
            recordStore.delete(dataKey);
            mapServiceContext.interceptAfterRemove(mapName, oldValue);
            stats.incrementRemoveLatencyNanos(getLatencyNanos(startTimeNanos));
        }
    }

    private static long getLatencyNanos(long beginTimeNanos) {
        return System.nanoTime() - beginTimeNanos;
    }

    private void process(Map.Entry entry) {
        if (backup) {
            backupProcessor.processBackup(entry);
            return;
        }

        result = ss.toData(entryProcessor.process(entry));
    }

    private void throwModificationInReadOnlyException() {
        throw new UnsupportedOperationException("Entry Processor " + entryProcessor.getClass().getName()
                + " marked as ReadOnly tried to modify map " + mapName + ". This is not supported. Remove "
                + "the ReadOnly marker from the Entry Processor or do not modify the entry in the process "
                + "method.");
    }

    private void publishWanReplicationEvent() {
        assert entryWasModified();

        Data dataKey = toHeapData(this.dataKey);

        if (eventType == REMOVED) {
            if (backup) {
                mapEventPublisher.publishWanReplicationRemoveBackup(mapName, dataKey, Clock.currentTimeMillis());
            } else {
                mapEventPublisher.publishWanReplicationRemove(mapName, dataKey, Clock.currentTimeMillis());
                if (collectWanEvents) {
                    wanEventList.add(new WanEventHolder(dataKey, null, REMOVED));
                }
            }

            return;
        }

        Record record = recordStore.getRecord(dataKey);
        Data dataNewValue = toHeapData(ss.toData(newValue));
        EntryView entryView = createSimpleEntryView(dataKey, dataNewValue, record);
        if (backup) {
            mapEventPublisher.publishWanReplicationUpdateBackup(mapName, entryView);
        } else {
            mapEventPublisher.publishWanReplicationUpdate(mapName, entryView);
            if (collectWanEvents) {
                wanEventList.add(new WanEventHolder(dataKey, dataNewValue, UPDATED));
            }
        }
    }

    private void publishEntryEvent() {
        Object oldValue = getOrNullOldValue();
        mapEventPublisher.publishEvent(callerAddress, mapName, eventType, toHeapData(dataKey), oldValue, newValue);
    }

    /**
     * Nullify old value if in memory format is object and operation is not removal
     * since old and new value in fired event {@link com.hazelcast.core.EntryEvent}
     * may be same due to the object in memory format.
     */
    private Object getOrNullOldValue() {
        if (inMemoryFormat == OBJECT && eventType != REMOVED) {
            return null;
        } else {
            return oldValue;
        }
    }
}
