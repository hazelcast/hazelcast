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

import com.hazelcast.collection.CollectionBackupAwareOperation;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import java.io.IOException;

public class ListSetOperation extends CollectionBackupAwareOperation {

    private int index;
    private Data value;
    private long itemId = -1;
    private long oldItemId = -1;

    public ListSetOperation() {
    }

    public ListSetOperation(String name, int index, Data value) {
        super(name);
        this.index = index;
        this.value = value;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new ListSetBackupOperation(name, oldItemId, itemId, value);
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.LIST_SET;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        final ListContainer container = getOrCreateListContainer();
        itemId = container.nextId();
        final CollectionItem item = container.set(index, itemId, value);
        oldItemId = item.getItemId();
        response = item.getValue();
    }

    @Override
    public void afterRun() throws Exception {
        publishEvent(ItemEventType.REMOVED, (Data) response);
        publishEvent(ItemEventType.ADDED, value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
        value = in.readData();
    }
}
