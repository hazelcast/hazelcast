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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import java.io.IOException;

public class CollectionAddOperation extends CollectionBackupAwareOperation {

    protected Data value;
    protected long itemId = -1;

    public CollectionAddOperation() {
    }

    public CollectionAddOperation(String name, Data value) {
        super(name);
        this.value = value;
    }

    @Override
    public boolean shouldBackup() {
        return itemId != -1;
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionAddBackupOperation(name, itemId, value);
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ADD;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        if (hasEnoughCapacity(1)) {
            itemId = getOrCreateContainer().add(value);
        }
        response = itemId != -1;
    }

    @Override
    public void afterRun() throws Exception {
        if (itemId != -1) {
            publishEvent(ItemEventType.ADDED, value);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        value.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = new Data();
        value.readData(in);
    }
}
