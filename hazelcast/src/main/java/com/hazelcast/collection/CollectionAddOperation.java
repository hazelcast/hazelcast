/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

/**
 * @ali 9/4/13
 */
public class CollectionAddOperation extends CollectionBackupAwareOperation {

    protected Data value;

    protected transient long itemId;

    public CollectionAddOperation() {
    }

    public CollectionAddOperation(String name, Data value) {
        super(name);
        this.value = value;
    }

    public boolean shouldBackup() {
        return itemId != -1;
    }

    public Operation getBackupOperation() {
        return new CollectionAddBackupOperation(name, itemId, value);
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ADD;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        itemId = getOrCreateContainer().add(value);
    }

    public void afterRun() throws Exception {
        if (itemId != -1){
            publishEvent(ItemEventType.ADDED, value);
        }
    }
}
