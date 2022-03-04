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
import com.hazelcast.collection.impl.collection.operations.CollectionAddBackupOperation;
import com.hazelcast.collection.impl.collection.operations.CollectionAddOperation;
import com.hazelcast.collection.impl.list.ListContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

public class ListAddOperation extends CollectionAddOperation {

    private int index = -1;

    public ListAddOperation() {
    }

    public ListAddOperation(String name, int index, Data value) {
        super(name, value);
        this.index = index;
    }

    @Override
    public void run() throws Exception {
        final ListContainer listContainer = getOrCreateListContainer();
        response = false;
        if (!hasEnoughCapacity(1)) {
            return;
        }

        final CollectionItem item = listContainer.add(index, value);
        if (item != null) {
            itemId = item.getItemId();
            response = true;
        }
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionAddBackupOperation(name, itemId, value);
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.LIST_ADD;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
    }
}
