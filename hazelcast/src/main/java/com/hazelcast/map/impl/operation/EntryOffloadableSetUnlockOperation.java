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

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.util.Clock;

import java.io.IOException;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;

/**
 * Set & Unlock processing for the EntryOperation
 *
 * See the javadoc on {@link EntryOperation}
 */
public class EntryOffloadableSetUnlockOperation extends MutatingKeyBasedMapOperation implements BackupAwareOperation, Notifier {

    protected Data value;
    protected Data oldValue;
    protected String caller;
    protected long begin;
    protected EntryEventType modificationType;
    protected EntryBackupProcessor entryBackupProcessor;

    public EntryOffloadableSetUnlockOperation() {
    }

    public EntryOffloadableSetUnlockOperation(String name, EntryEventType modificationType, Data key, Data oldValue,
                                              Data value, String caller, long threadId, long begin,
                                              EntryBackupProcessor entryBackupProcessor) {
        super(name, key, value);
        this.value = value;
        this.oldValue = oldValue;
        this.caller = caller;
        this.begin = begin;
        this.modificationType = modificationType;
        this.entryBackupProcessor = entryBackupProcessor;
        this.setThreadId(threadId);
    }

    @Override
    public void run() throws Exception {
        verifyLock();
        try {
            updateRecordStore();
        } finally {
            unlockKey();
        }
    }

    private void verifyLock() {
        if (!recordStore.isLockedBy(dataKey, caller, threadId)) {
            // we can't send a RetryableHazelcastException explicitly since it would retry this opertation and we want to retry
            // the preceding EntryOperation that this operation is part of.
            throw new EntryOffloadableLockMismatchException(
                    String.format("The key is not locked by the caller=%s and threadId=%d", caller, threadId));
        }
    }

    private void updateRecordStore() {
        if (modificationType == null) {
            return;
        }
        if (modificationType == REMOVED) {
            recordStore.remove(dataKey);
            getLocalMapStats().incrementRemoves(getLatencyFrom(begin));
        } else if (modificationType == ADDED || modificationType == UPDATED) {
            recordStore.set(dataKey, value, DEFAULT_TTL);
            getLocalMapStats().incrementPuts(getLatencyFrom(begin));
        } else {
            throw new IllegalArgumentException("Unsupported event type " + modificationType);
        }
    }

    private void unlockKey() {
        boolean unlocked = recordStore.unlock(dataKey, caller, threadId, getCallId());
        if (!unlocked) {
            throw new IllegalStateException(
                    String.format("Unexpected error! EntryOffloadableSetUnlockOperation finished but the unlock method "
                            + "returned false for caller=%s and threadId=%d", caller, threadId));
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        if (modificationType == null) {
            return;
        }

        mapServiceContext.interceptAfterPut(name, value);
        if (isPostProcessing(recordStore)) {
            Record record = recordStore.getRecord(dataKey);
            value = record == null ? null : toData(record.getValue());
        }
        invalidateNearCache(dataKey);
        publishEntryEvent();
        publishWanReplicationEvent();
        evict(dataKey);
    }

    private void publishWanReplicationEvent() {
        final MapContainer mapContainer = this.mapContainer;
        if (mapContainer.getWanReplicationPublisher() == null && mapContainer.getWanMergePolicy() == null) {
            return;
        }
        final Data key = getKey();

        if (REMOVED.equals(modificationType)) {
            mapEventPublisher.publishWanReplicationRemove(name, key, begin);

        } else {
            final Record record = recordStore.getRecord(key);
            if (record != null) {
                final EntryView entryView = createSimpleEntryView(key, value, record);
                mapEventPublisher.publishWanReplicationUpdate(name, entryView);
            }
        }
    }

    private void publishEntryEvent() {
        if (hasRegisteredListenerForThisMap()) {
            nullifyOldValueIfNecessary();
            mapEventPublisher.publishEvent(getCallerAddress(), name, modificationType, dataKey, oldValue, value);
        }
    }

    private boolean hasRegisteredListenerForThisMap() {
        final EventService eventService = getNodeEngine().getEventService();
        return eventService.hasEventRegistration(SERVICE_NAME, name);
    }

    /**
     * Nullify old value if in memory format is object and operation is not removal
     * since old and new value in fired event {@link com.hazelcast.core.EntryEvent}
     * may be same due to the object in memory format.
     */
    private void nullifyOldValueIfNecessary() {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final InMemoryFormat format = mapConfig.getInMemoryFormat();
        if (format == InMemoryFormat.OBJECT && modificationType != REMOVED) {
            oldValue = null;
        }
    }

    private Data toData(Object obj) {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.toData(obj);
    }

    private long getLatencyFrom(long begin) {
        return Clock.currentTimeMillis() - begin;
    }

    @Override
    public boolean returnsResponse() {
        // this has to be true, otherwise the calling side won't be notified about the exception thrown by this operation
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return entryBackupProcessor != null ? new EntryBackupOperation(name, dataKey, entryBackupProcessor) : null;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryBackupProcessor != null;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return new LockWaitNotifyKey(new DefaultObjectNamespace(MapService.SERVICE_NAME, name), dataKey);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.ENTRY_OFFLOADABLE_SET_UNLOCK;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(modificationType != null ? modificationType.name() : "");
        out.writeData(oldValue);
        out.writeData(value);
        out.writeUTF(caller);
        out.writeLong(begin);
        out.writeObject(entryBackupProcessor);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        String modificationTypeName = in.readUTF();
        modificationType = modificationTypeName.equals("") ? null : EntryEventType.valueOf(modificationTypeName);
        oldValue = in.readData();
        value = in.readData();
        caller = in.readUTF();
        begin = in.readLong();
        entryBackupProcessor = in.readObject();
    }

}
