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

package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.CollectionRemoveBackupOperation;
import com.hazelcast.collection.CollectionBackupAwareOperation;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 9/1/13
 */
public class ListRemoveOperation extends CollectionBackupAwareOperation {

    private int index;

    private transient long itemId;

    public ListRemoveOperation() {
    }

    public ListRemoveOperation(String name, int index) {
        super(name);
        this.index = index;
    }

    public boolean shouldBackup() {
        return true;
    }

    public Operation getBackupOperation() {
        return new CollectionRemoveBackupOperation(name, itemId);
    }

    public int getId() {
        return CollectionDataSerializerHook.LIST_REMOVE;
    }

    public void beforeRun() throws Exception {
        publishEvent(ItemEventType.ADDED, (Data)response);
    }

    public void run() throws Exception {
        final CollectionItem item = getOrCreateListContainer().remove(index);
        itemId = item.getItemId();
        response = item.getValue();
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
    }
}
