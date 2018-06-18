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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

public class SetTTLBackupOperation extends KeyBasedMapOperation implements BackupOperation {

    public SetTTLBackupOperation() {

    }

    public SetTTLBackupOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey, ttl);
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.SET_TTL_BACKUP;
    }

    @Override
    public void run() throws Exception {
        recordStore.setTTL(dataKey, ttl);
    }

    @Override
    public void afterRun() throws Exception {
        Record record = recordStore.getRecord(dataKey);
        if (record == null) {
            return;
        }
        if (mapContainer.isWanReplicationEnabled()) {
            EntryView entryView = EntryViews.toSimpleEntryView(record);
            mapEventPublisher.publishWanUpdate(name, entryView);
        }
    }
}
