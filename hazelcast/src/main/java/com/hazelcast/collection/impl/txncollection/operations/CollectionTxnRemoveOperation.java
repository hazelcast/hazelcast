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

package com.hazelcast.collection.impl.txncollection.operations;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.collection.operations.CollectionBackupAwareOperation;
import com.hazelcast.collection.impl.txncollection.CollectionTxnOperation;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

public class CollectionTxnRemoveOperation extends CollectionBackupAwareOperation implements CollectionTxnOperation,
        MutatingOperation {

    private long itemId;
    private transient CollectionItem item;

    public CollectionTxnRemoveOperation() {
    }

    public CollectionTxnRemoveOperation(String name, long itemId) {
        super(name);
        this.itemId = itemId;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionTxnRemoveBackupOperation(name, itemId);
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        item = collectionContainer.commitRemove(itemId);
    }

    @Override
    public void afterRun() throws Exception {
        if (item != null) {
            publishEvent(ItemEventType.REMOVED, item.getValue());
        }
    }

    @Override
    public long getItemId() {
        return itemId;
    }

    @Override
    public boolean isRemoveOperation() {
        return true;
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_TXN_REMOVE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
    }
}
