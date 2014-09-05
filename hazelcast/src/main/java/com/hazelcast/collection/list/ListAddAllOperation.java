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

import com.hazelcast.collection.CollectionAddAllOperation;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import java.io.IOException;
import java.util.List;

public class ListAddAllOperation extends CollectionAddAllOperation {

    private int index = -1;

    public ListAddAllOperation() {
    }

    public ListAddAllOperation(String name, int index, List<Data> valueList) {
        super(name, valueList);
        this.index = index;
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.LIST_ADD_ALL;
    }

    @Override
    public void run() throws Exception {
        if (!hasEnoughCapacity(valueList.size())) {
            response = false;
            return;
        }
        valueMap = getOrCreateListContainer().addAll(index, valueList);
        response = !valueMap.isEmpty();
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
