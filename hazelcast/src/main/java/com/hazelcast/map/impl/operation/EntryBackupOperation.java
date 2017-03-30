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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.LockAwareLazyMapEntry;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;

/**
 * @see EntryOperation for Offloadable support.
 */
public class EntryBackupOperation extends MutatingKeyBasedMapOperation implements BackupOperation {

    protected transient Object oldValue;
    private EntryBackupProcessor entryProcessor;

    public EntryBackupOperation() {
    }

    public EntryBackupOperation(String name, Data dataKey, EntryBackupProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        if (entryProcessor instanceof HazelcastInstanceAware) {
            HazelcastInstance hazelcastInstance = getNodeEngine().getHazelcastInstance();
            ((HazelcastInstanceAware) entryProcessor).setHazelcastInstance(hazelcastInstance);
        }
    }

    @Override
    public void run() {
        boolean shouldClone = mapContainer.shouldCloneOnEntryProcessing();
        SerializationService serializationService = getNodeEngine().getSerializationService();
        oldValue = recordStore.get(dataKey, true);
        Object value = shouldClone ? serializationService.toObject(serializationService.toData(oldValue)) : oldValue;

        Map.Entry entry = createMapEntry(dataKey, value);

        entryProcessor.processBackup(entry);

        if (noOp(entry, oldValue)) {
            return;
        }

        if (entryRemovedBackup(entry)) {
            return;
        }

        entryAddedOrUpdatedBackup(entry);
    }

    private void publishWanReplicationEvent(EntryEventType eventType) {
        final MapContainer mapContainer = this.mapContainer;
        if (!mapContainer.isWanReplicationEnabled()) {
            return;
        }
        final MapEventPublisher mapEventPublisher = mapContainer.getMapServiceContext().getMapEventPublisher();
        final Data key = dataKey;

        if (EntryEventType.REMOVED == eventType) {
            mapEventPublisher.publishWanReplicationRemoveBackup(name, key, Clock.currentTimeMillis());
        } else {
            final Record record = recordStore.getRecord(key);
            if (record != null) {
                dataValue = mapContainer.getMapServiceContext().toData(dataValue);
                final EntryView entryView = createSimpleEntryView(key, dataValue, record);
                mapEventPublisher.publishWanReplicationUpdateBackup(name, entryView);
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
        evict(dataKey);
    }

    private boolean entryRemovedBackup(Map.Entry entry) {
        final Object value = entry.getValue();
        if (value == null) {
            recordStore.removeBackup(dataKey);
            publishWanReplicationEvent(EntryEventType.REMOVED);
            return true;
        }
        return false;
    }

    private void entryAddedOrUpdatedBackup(Map.Entry entry) {
        Object value = entry.getValue();
        recordStore.putBackup(dataKey, value);
        publishWanReplicationEvent(EntryEventType.UPDATED);
    }

    private Map.Entry createMapEntry(Data key, Object value) {
        InternalSerializationService serializationService
                = ((InternalSerializationService) getNodeEngine().getSerializationService());
        boolean locked = recordStore.isLocked(key);
        return new LockAwareLazyMapEntry(key, value, serializationService, mapContainer.getExtractors(), locked);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.ENTRY_BACKUP;
    }
}
