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
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;

/**
 * Abstract class that provides common backup post-ops
 * <p/>
 * Backup operations of operations that extends {@link AbstractMultipleEntryOperation} should
 * extend this class.
 * <p/>
 * Common functions for these classes can be moved to this class. For now, it only overrides
 * {@link AbstractMultipleEntryOperation#afterRun} method to publish backups of wan replication events.
 */
abstract class AbstractMultipleEntryBackupOperation extends AbstractMultipleEntryOperation {

    protected AbstractMultipleEntryBackupOperation() {
    }

    protected AbstractMultipleEntryBackupOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name, backupProcessor);
    }

    @Override
    public void afterRun() throws Exception {
        publishWanReplicationEventBackups();
    }

    protected void publishWanReplicationEventBackups() {
        for (WanEventWrapper wanEventWrapper : wanEventList) {
            publishWanReplicationEventBackup(wanEventWrapper.getKey(),
                    wanEventWrapper.getValue(), wanEventWrapper.getEventType());
        }
    }

    protected void publishWanReplicationEventBackup(Data key, Object value, EntryEventType eventType) {
        if (mapContainer.isWanReplicationEnabled()) {
            if (REMOVED.equals(eventType)) {
                mapEventPublisher.publishWanReplicationRemoveBackup(name, key, getNow());
            } else {
                final Record record = recordStore.getRecord(key);
                if (record != null) {
                    final Data dataValueAsData = toData(value);
                    final EntryView entryView = createSimpleEntryView(key, dataValueAsData, record);
                    mapEventPublisher.publishWanReplicationUpdateBackup(name, entryView);
                }
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(wanEventList.size());
        for (WanEventWrapper wanEventWrapper : wanEventList) {
            out.writeData(wanEventWrapper.getKey());
            out.writeData(wanEventWrapper.getValue());
            out.writeInt(wanEventWrapper.getEventType().getType());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            Data value = in.readData();
            EntryEventType entryEventType = EntryEventType.getByType(in.readInt());
            wanEventList.add(new WanEventWrapper(key, value, entryEventType));
        }
    }
}
