/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.list.operations;

import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.collection.operations.CollectionBackupAwareOperation;
import com.hazelcast.collection.impl.list.ListContainer;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

import static com.hazelcast.collection.impl.collection.CollectionContainer.INVALID_ITEM_ID;

public class ListSetOperation extends CollectionBackupAwareOperation implements MutatingOperation {

    private int index;
    private Data value;
    private long itemId = INVALID_ITEM_ID;
    private long oldItemId = INVALID_ITEM_ID;

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
    public void run() throws Exception {
        ListContainer listContainer = getOrCreateListContainer();
        itemId = listContainer.nextId();
        CollectionItem item = listContainer.set(index, itemId, value);
        oldItemId = item.getItemId();
        response = item.getValue();
    }

    @Override
    public void afterRun() throws Exception {
        publishEvent(ItemEventType.REMOVED, (Data) response);
        publishEvent(ItemEventType.ADDED, value);
        super.afterRun();
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.LIST_SET;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
        IOUtil.writeData(out, value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
        value = IOUtil.readData(in);
    }
}
