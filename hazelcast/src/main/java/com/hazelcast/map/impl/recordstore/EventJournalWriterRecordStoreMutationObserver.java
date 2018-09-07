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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.journal.MapEventJournal;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ObjectNamespace;

public class EventJournalWriterRecordStoreMutationObserver implements RecordStoreMutationObserver {
    private final MapEventJournal eventJournal;
    private final int partitionId;
    private final EventJournalConfig eventJournalConfig;
    private final ObjectNamespace objectNamespace;

    public EventJournalWriterRecordStoreMutationObserver(MapEventJournal eventJournal, MapContainer mapContainer,
                                                         int partitionId) {
        this.eventJournal = eventJournal;
        this.partitionId = partitionId;
        this.eventJournalConfig = mapContainer.getEventJournalConfig();
        this.objectNamespace = mapContainer.getObjectNamespace();
    }

    @Override
    public void onClear() {
        // NOP
    }

    @Override
    public void onPutRecord(Data key, Record record) {
        eventJournal.writeAddEvent(eventJournalConfig, objectNamespace, partitionId, record.getKey(), record.getValue());
    }

    @Override
    public void onReplicationPutRecord(Data key, Record record) {
        // NOP
    }

    @Override
    public void onUpdateRecord(Data key, Record record, Object newValue) {
        eventJournal.writeUpdateEvent(eventJournalConfig, objectNamespace, partitionId,
                record.getKey(), record.getValue(), newValue);
    }

    @Override
    public void onRemoveRecord(Data key, Record record) {
        eventJournal.writeRemoveEvent(eventJournalConfig, objectNamespace, partitionId, record.getKey(), record.getValue());
    }

    @Override
    public void onEvictRecord(Data key, Record record) {
        eventJournal.writeEvictEvent(eventJournalConfig, objectNamespace, partitionId, record.getKey(), record.getValue());
    }

    @Override
    public void onLoadRecord(Data key, Record record) {
        eventJournal.writeLoadEvent(eventJournalConfig, objectNamespace, partitionId, record.getKey(), record.getValue());
    }

    @Override
    public void onDestroy(boolean internal) {
        if (!internal) {
            eventJournal.destroy(objectNamespace, partitionId);
        }
    }

    @Override
    public void onReset() {
        // NOP
    }
}
