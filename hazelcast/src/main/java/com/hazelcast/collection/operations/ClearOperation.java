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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.util.Collection;
import java.util.Map;

/**
 * @ali 1/9/13
 */
public class ClearOperation extends CollectionOperation implements BackupAwareOperation, PartitionAwareOperation {

    transient Map<Data, Collection<CollectionRecord>> objects;

    public ClearOperation() {
    }

    public ClearOperation(CollectionProxyId proxyId) {
        super(proxyId);
    }

    public void beforeRun() throws Exception {
        if (hasListener()) {
            CollectionContainer container = getOrCreateContainer();
            objects = container.copyCollections();
        }
    }

    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        container.clearCollections();
        response = true;
    }

    public void afterRun() throws Exception {
        if (objects != null && !objects.isEmpty()) {
            CollectionContainer container = getOrCreateContainer();
            for (Map.Entry<Data, Collection<CollectionRecord>> entry : objects.entrySet()) {
                Data key = entry.getKey();
                if (container.isLocked(key)) {
                    continue;//key is locked so not removed
                }
                Collection<CollectionRecord> coll = entry.getValue();
                for (CollectionRecord record : coll) {
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
        return new ClearBackupOperation(proxyId);
    }

    public int getId() {
        return CollectionDataSerializerHook.CLEAR;
    }

}
