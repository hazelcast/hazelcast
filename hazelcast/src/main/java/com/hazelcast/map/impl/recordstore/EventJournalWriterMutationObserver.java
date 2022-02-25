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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.journal.MapEventJournal;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.internal.serialization.Data;

import javax.annotation.Nonnull;

public class EventJournalWriterMutationObserver implements MutationObserver {

    private final int partitionId;
    private final MapEventJournal eventJournal;
    private final EventJournalConfig eventJournalConfig;
    private final ObjectNamespace objectNamespace;

    public EventJournalWriterMutationObserver(MapEventJournal eventJournal,
                                              MapContainer mapContainer,
                                              int partitionId) {
        this.eventJournal = eventJournal;
        this.partitionId = partitionId;
        this.eventJournalConfig = mapContainer.getEventJournalConfig();
        this.objectNamespace = mapContainer.getObjectNamespace();
    }

    @Override
    public void onPutRecord(@Nonnull Data key, Record record, Object oldValue, boolean backup) {
        eventJournal.writeAddEvent(eventJournalConfig, objectNamespace,
                partitionId, key, record.getValue());
    }


    @Override
    public void onUpdateRecord(@Nonnull Data key, @Nonnull Record record,
                               Object oldValue, Object newValue, boolean backup) {
        eventJournal.writeUpdateEvent(eventJournalConfig, objectNamespace,
                partitionId, key, oldValue, newValue);
    }

    @Override
    public void onRemoveRecord(Data key, Record record) {
        eventJournal.writeRemoveEvent(eventJournalConfig, objectNamespace,
                partitionId, key, record.getValue());
    }

    @Override
    public void onEvictRecord(Data key, Record record) {
        eventJournal.writeEvictEvent(eventJournalConfig, objectNamespace,
                partitionId, key, record.getValue());
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull Record record, boolean backup) {
        eventJournal.writeLoadEvent(eventJournalConfig, objectNamespace,
                partitionId, key, record.getValue());
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        if (!internal) {
            eventJournal.destroy(objectNamespace, partitionId);
        }
    }

    @Override
    public void onReplicationPutRecord(@Nonnull Data key, @Nonnull Record record, boolean populateIndex) {
        // NOP
    }

    @Override
    public void onClear() {
        // NOP
    }

    @Override
    public void onReset() {
        // NOP
    }
}
