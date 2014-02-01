/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.operations;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.multimap.MultiMapContainer;
import com.hazelcast.multimap.MultiMapDataSerializerHook;
import com.hazelcast.multimap.MultiMapRecord;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.util.Collection;
import java.util.Map;

/**
 * @author ali 1/9/13
 */
public class ClearOperation extends MultiMapOperation implements BackupAwareOperation, PartitionAwareOperation {

    Map<Data, Collection<MultiMapRecord>> objects;

    public ClearOperation() {
    }

    public ClearOperation(String name) {
        super(name);
    }

    public void beforeRun() throws Exception {
        if (hasListener()) {
            MultiMapContainer container = getOrCreateContainer();
            objects = container.copyCollections();
        }
    }

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        container.clear();
        response = true;
    }

    public void afterRun() throws Exception {
        ((MultiMapService) getService()).getLocalMultiMapStatsImpl(name).incrementOtherOperations();

        if (objects != null && !objects.isEmpty()) {
            MultiMapContainer container = getOrCreateContainer();
            for (Map.Entry<Data, Collection<MultiMapRecord>> entry : objects.entrySet()) {
                Data key = entry.getKey();
                if (container.isLocked(key)) {
                    continue;//key is locked so not removed
                }
                Collection<MultiMapRecord> coll = entry.getValue();
                for (MultiMapRecord record : coll) {
                    publishEvent(EntryEventType.REMOVED, key, record.getObject());
                }
            }
            objects.clear();
        }
        objects = null;
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(objects != null && !objects.isEmpty());
    }

    public Operation getBackupOperation() {
        return new ClearBackupOperation(name);
    }

    public int getId() {
        return MultiMapDataSerializerHook.CLEAR;
    }

}
