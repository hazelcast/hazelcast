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

package com.hazelcast.collection;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.util.Map;

/**
 * @ali 8/31/13
 */
public class CollectionClearOperation extends CollectionBackupAwareOperation {

    private transient Map<Long, Data> itemIdMap;


    public CollectionClearOperation() {
    }

    public CollectionClearOperation(String name) {
        super(name);
    }

    public boolean shouldBackup() {
        return itemIdMap != null && !itemIdMap.isEmpty();
    }

    public Operation getBackupOperation() {
        return new CollectionClearBackupOperation(name, itemIdMap.keySet());
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_CLEAR;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        itemIdMap = getOrCreateContainer().clear();
    }

    public void afterRun() throws Exception {
        for (Data value : itemIdMap.values()) {
            publishEvent(ItemEventType.REMOVED, value);
        }
    }
}
