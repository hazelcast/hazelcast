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

package com.hazelcast.collection.impl.collection.operations;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.util.Map;

public class CollectionClearOperation extends CollectionBackupAwareOperation implements MutatingOperation {

    private Map<Long, Data> itemIdMap;

    public CollectionClearOperation() {
    }

    public CollectionClearOperation(String name) {
        super(name);
    }

    @Override
    public boolean shouldBackup() {
        return itemIdMap != null && !itemIdMap.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionClearBackupOperation(name, itemIdMap.keySet());
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        itemIdMap = collectionContainer.clear();
    }

    @Override
    public void afterRun() throws Exception {
        for (Data value : itemIdMap.values()) {
            publishEvent(ItemEventType.REMOVED, value);
        }
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_CLEAR;
    }
}
