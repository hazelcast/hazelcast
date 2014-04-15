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

import com.hazelcast.collection.CollectionAddBackupOperation;
import com.hazelcast.collection.CollectionAddOperation;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
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
        final ListContainer container = getOrCreateListContainer();
        response = false;
        if (!hasEnoughCapacity(1)) {
            return;
        }
        final CollectionItem item = container.add(index, value);
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
    public int getId() {
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
